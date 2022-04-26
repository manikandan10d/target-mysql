import json
import sys
import singer
import collections
import inflection
import re
import itertools
import os
import shutil
import pymysql
import pymysql.cursors
from datetime import datetime

import pandas as pd

'''
from python >= v3.10 collections.MutableMapping moved to abc.MutableMapping,
so we need to give fix to support all python versions.
'''
try:
    from collections import abc
    collections.MutableMapping = abc.MutableMapping
except:
    pass

TARGET_REJECTED_DIR = os.getenv("TARGET_REJECTED_DIR")
NULL_TYPE = {'type': 'null'}

logger = singer.get_logger()

def column_type(schema_property):
    property_type = schema_property['type']
    property_format = schema_property['format'] if 'format' in schema_property else None
    maxLength = schema_property['maxLength'] if 'maxLength' in schema_property else 255
    if 'object' in property_type or 'array' in property_type:
        return 'jsonb'
    elif property_format == 'date-time':
        return 'datetime'
    elif 'number' in property_type:
        return 'numeric'
    elif 'integer' in property_type and 'string' in property_type:
        return 'varchar({})'.format(maxLength)
    elif 'integer' in property_type:
        return 'bigint'
    elif 'boolean' in property_type:
        return 'boolean'
    else:
        return 'varchar({})'.format(maxLength)


def inflect_column_name(name):
    name = re.sub(r"([A-Z]+)_([A-Z][a-z])", r'\1__\2', name)
    name = re.sub(r"([a-z\d])_([A-Z])", r'\1__\2', name)
    return inflection.underscore(name)


def safe_column_name(name):
    return '`{}`'.format(name)


def column_clause(name, schema_property):
    return '{} {}'.format(safe_column_name(name), column_type(schema_property))


def flatten_key(k, parent_key, sep):
    full_key = parent_key + [k]
    inflected_key = [inflect_column_name(n) for n in full_key]
    reducer_index = 0
    while len(sep.join(inflected_key)) >= 63 and reducer_index < len(inflected_key):
        reduced_key = re.sub(r'[a-z]', '', inflection.camelize(inflected_key[reducer_index]))
        inflected_key[reducer_index] = \
            (reduced_key if len(reduced_key) > 1 else inflected_key[reducer_index][0:3]).lower()
        reducer_index += 1

    return sep.join(inflected_key)


def flatten_schema(d, parent_key=[], sep='__'):
    items = []
    for k, v in d['properties'].items():
        new_key = flatten_key(k, parent_key, sep)

        if not v:
            logger.warn("Empty definition for {}.".format(new_key))
            continue

        if 'type' in v.keys():
            if 'object' in v['type']:
                items.extend(flatten_schema(v, parent_key + [k], sep=sep).items())
            else:
                items.append((new_key, v))
        elif 'anyOf' in v.keys():
            properties = list(v.values())[0]
            if NULL_TYPE not in properties or len(properties) > 2:
                raise ValueError('Unsupported column type anyOf: {}'.format(k))
            property = list(filter(lambda x: x != NULL_TYPE, properties))[0]
            if not isinstance(property['type'], list):
                property['type'] = ['null', property['type']]
            items.append((new_key, property))
        else:
            raise ValueError('Unsupported column type: {}'.format(k))

    key_func = lambda item: item[0]
    sorted_items = sorted(items, key=key_func)
    for k, g in itertools.groupby(sorted_items, key=key_func):
        if len(list(g)) > 1:
            raise ValueError('Duplicate column name produced in schema: {}'.format(k))

    return dict(sorted_items)


def flatten_record(d, parent_key=[], sep='__'):
    items = []
    for k, v in d.items():
        new_key = flatten_key(k, parent_key, sep)
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten_record(v, parent_key + [k], sep=sep).items())
        else:
            items.append((new_key, json.dumps(v) if type(v) is list else v))
    return dict(items)


def primary_column_names(stream_schema_message):
    return [
        safe_column_name(inflect_column_name(p))
        for p in stream_schema_message['key_properties']
    ]


def transform_to_mysqldate(val):
    MYSQL_DTAETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    dt = datetime.strptime(val,'%Y-%m-%dT%H:%M:%S.%fZ')
    return dt.strftime(MYSQL_DTAETIME_FORMAT)

def stream_name_to_dict(stream_name, separator='-'):
    catalog_name = None
    schema_name = None
    table_name = stream_name

    # Schema and table name can be derived from stream if it's in <schema_nama>-<table_name> format
    s = stream_name.split(separator)
    if len(s) == 2:
        schema_name = s[0]
        table_name = s[1]
    if len(s) > 2:
        catalog_name = s[0]
        schema_name = s[1]
        table_name = '_'.join(s[2:])

    return {
        'catalog_name': catalog_name,
        'schema_name': schema_name,
        'table_name': table_name
    }

class DbSync:

    def __init__(self, connection_config, stream_schema_message):
        self.connection_config = connection_config
        self.schema_name = self.connection_config['schema']
        self.stream_schema_message = stream_schema_message
        self.flatten_schema = flatten_schema(stream_schema_message['schema'])
        self.rejected_count = 0
    
    def open_connection(self):

        return pymysql.connect( host=self.connection_config['host'],
                                user=self.connection_config['user'],
                                password=self.connection_config['password'],
                                database=self.connection_config['dbname'],
                                port=self.connection_config['port'],
                                cursorclass=pymysql.cursors.DictCursor)

    def query(self, query, params=None):
        connection = self.open_connection()
        with connection:
            with connection.cursor() as cur:
                cur.execute(
                    query,
                    params
                )

                if cur.rowcount > 0:
                    return cur.fetchall()
                else:
                    return []

    # Need to check its useful or not
    def copy_from(self, file, table):
        with self.open_connection() as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cur:
                cur.copy_from(file, table)

    def table_name(self, stream_name, is_temporary=False, without_schema=False):
        stream_dict = stream_name_to_dict(stream_name)
        table_name = stream_dict['table_name']
        table_name = table_name.replace('.', '_').replace('-', '_')

        if is_temporary:
            return '{}_temp'.format(table_name)
        else:
            if without_schema:
                return '{}'.format(table_name)
            else:
                return '{}.{}'.format(self.schema_name, table_name)

    # Need to check its useful or not
    def reject_file(self, file):
        self.rejected_count += 1

        if not TARGET_REJECTED_DIR:
            return

        os.makedirs(TARGET_REJECTED_DIR, exist_ok=True)
        rejected_file_name = "{}-{:04d}.rej.csv".format(self.stream_schema_message['stream'],
                                                    self.rejected_count)
        rejected_file_path = os.path.join(TARGET_REJECTED_DIR,
                                          rejected_file_name)

        shutil.copy(file.name, rejected_file_path)
        logger.info("Saved rejected entries as {}".format(rejected_file_path))

    def record_primary_key_string(self, record):
        if len(self.stream_schema_message['key_properties']) == 0:
            return None
        flatten = flatten_record(record)
        key_props = [str(flatten[inflect_column_name(p)]) for p in self.stream_schema_message['key_properties']]
        return ','.join(key_props)

    def record_to_csv_line(self, record):
        flatten = flatten_record(record)
        records=[]
        for name in self.flatten_schema:
            if name in flatten and flatten[name] is not None:
                value = flatten[name]
                value_format = self.flatten_schema[name]['format'] if 'format' in self.flatten_schema[name] else ''
                if value_format == 'date-time':
                    records.append(transform_to_mysqldate(value))
                else:
                    if type(value) is not str:
                        records.append(F'{value}')
                    else:
                        records.append(value)
            else:
                records.append('')
        
        return ','.join(records)

    def load_csv(self, file, count):
        try:
            file.seek(0)
            stream_schema_message = self.stream_schema_message
            stream = stream_schema_message['stream']
            logger.info("Loading {} rows into '{}'".format(count, stream))
            connection = self.open_connection()
            with connection:
                with connection.cursor(pymysql.cursors.DictCursor) as cur:
                    cur.execute(self.create_table_query(True))
                    # Loading CSV file using Panda
                    df = pd.read_csv(file, header=None)
                    col_names = self.column_names()
                    sql = "INSERT INTO {}({}) VALUES({})".format(
                        self.table_name(stream, True),
                        ', '.join(col_names),
                        ','.join(["%s" for c in col_names])
                    )
                    logger.info(sql)
                    # Creating a list of tupples from the dataframe values
                    tpls = [tuple(x) for x in df.to_numpy()]
                    cur.executemany(sql, tpls)

                    if len(self.stream_schema_message['key_properties']) > 0:
                        cnt = cur.execute(self.update_from_temp_table())
                        logger.info(F'UPDATE {cnt} row')
                    cnt = cur.execute(self.insert_from_temp_table())
                    logger.info(F'INSERT {cnt} row')
                    cur.execute(self.drop_temp_table())
        except pymysql.Error as err:
            logger.exception("Failed to load CSV file: {}".format(file.name))
            self.reject_file(file)

    def insert_from_temp_table(self):
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        table = self.table_name(stream_schema_message['stream'], False)
        temp_table = self.table_name(stream_schema_message['stream'], True)

        if len(stream_schema_message['key_properties']) == 0:
            return """INSERT INTO {} ({})
                    (SELECT s.* FROM {} s)
                    """.format(
                table,
                ', '.join(columns),
                temp_table
            )

        return """INSERT INTO {} ({})
        (SELECT s.* FROM {} s LEFT OUTER JOIN {} t ON {} WHERE {})
        """.format(
            table,
            ', '.join(columns),
            temp_table,
            table,
            self.primary_key_condition('t'),
            self.primary_key_null_condition('t')
        )

    def update_from_temp_table(self):
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        table = self.table_name(stream_schema_message['stream'], False)
        temp_table = self.table_name(stream_schema_message['stream'], True)

        update_columns = ', '.join(['{}.{}=s.{}'.format(table, c, c) for c in columns])

        join_condition = self.primary_key_condition(table)

        update_sql = F'UPDATE {table} INNER JOIN {temp_table} s ON {join_condition} SET {update_columns}'

        return update_sql

    def primary_key_condition(self, right_table):
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(['s.{} = {}.{}'.format(c, right_table, c) for c in names])

    def primary_key_null_condition(self, right_table):
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(['{}.{} is null'.format(right_table, c) for c in names])

    def drop_temp_table(self):
        stream_schema_message = self.stream_schema_message
        temp_table = self.table_name(stream_schema_message['stream'], True)
        return "DROP TABLE {}".format(temp_table)

    def column_names(self):
        return [safe_column_name(name) for name in self.flatten_schema]

    def create_table_query(self, is_temporary=False):
        stream_schema_message = self.stream_schema_message
        columns = [
            column_clause(
                name,
                schema
            )
            for (name, schema) in self.flatten_schema.items()
        ]

        primary_key = ["PRIMARY KEY ({})".format(', '.join(primary_column_names(stream_schema_message)))] \
            if len(stream_schema_message['key_properties']) else []

        return 'CREATE {}TABLE {} ({})'.format(
            'TEMPORARY ' if is_temporary else '',
            self.table_name(stream_schema_message['stream'], is_temporary),
            ', '.join(columns + primary_key)
        )

    def create_schema_if_not_exists(self):
        schema_name = self.connection_config['schema']
        schema_rows = self.query(
            'SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s',
            (schema_name,)
        )

        if len(schema_rows) == 0:
            self.query("CREATE SCHEMA IF NOT EXISTS {}".format(schema_name))

    def get_tables(self):
        return self.query(
            'SELECT table_name FROM information_schema.tables WHERE table_schema = %s',
            (self.schema_name,)
        )

    def get_table_columns(self, table_name):
        return self.query("""SELECT column_name, data_type
      FROM information_schema.columns
      WHERE lower(table_name) = %s AND lower(table_schema) = %s""", (table_name.lower(), self.schema_name.lower()))

    def update_columns(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, without_schema=True)
        columns = self.get_table_columns(table_name)
        columns_dict = {column['column_name'.upper()].lower(): column for column in columns}

        columns_to_add = [
            column_clause(
                name,
                properties_schema
            )
            for (name, properties_schema) in self.flatten_schema.items()
            if name.lower() not in columns_dict
        ]

        for column in columns_to_add:
            self.add_column(column, stream)

        columns_to_replace = [
            (safe_column_name(name), column_clause(
                name,
                properties_schema
            ))
            for (name, properties_schema) in self.flatten_schema.items()
            if name.lower() in columns_dict and
               columns_dict[name.lower()]['data_type'.upper()].lower() != column_type(properties_schema).lower()
        ]

        for (column_name, column) in columns_to_replace:
            self.drop_column(column_name, stream)
            self.add_column(column, stream)

    def add_column(self, column, stream):
        add_column = "ALTER TABLE {} ADD COLUMN {}".format(self.table_name(stream, False), column)
        logger.info('Adding column: {}'.format(add_column))
        self.query(add_column)

    def drop_column(self, column_name, stream):
        drop_column = "ALTER TABLE {} DROP COLUMN {}".format(self.table_name(stream, False), column_name)
        logger.info('Dropping column: {}'.format(drop_column))
        self.query(drop_column)

    def sync_table(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']

        table_name = self.table_name(stream, without_schema=True)

        found_tables = [table for table in (self.get_tables()) if table['TABLE_NAME'].lower() == table_name.lower()]
        if len(found_tables) == 0:
            query = self.create_table_query()
            logger.info("Table '{}' does not exist. Creating... {}".format(table_name, query))
            self.query(query)
        else:
            logger.info("Table '{}' exists".format(table_name))
            self.update_columns()
