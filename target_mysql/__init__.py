#!/usr/bin/env python3

import argparse
import importlib
import io
import os
import sys
import json
import math
import threading
import http.client
import urllib
from datetime import datetime
import collections
from tempfile import NamedTemporaryFile

import decimal
from decimal import Decimal

import pkg_resources
from jsonschema import Draft4Validator, FormatChecker
import singer

from target_mysql.db_sync import DbSync

logger = singer.get_logger()

def float_to_decimal(value):
    '''Walk the given data structure and turn all instances of float into
    double.'''
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value

def numeric_schema_with_precision(schema):
    if 'type' not in schema:
        return False
    if isinstance(schema['type'], list):
        if 'number' not in schema['type']:
            return False
    elif schema['type'] != 'number':
        return False
    if 'multipleOf' in schema:
        return True
    return 'minimum' in schema or 'maximum' in schema

def flush_records(o, csv_files_to_load, row_count, primary_key_exists, sync):
    stream = o['stream']
    sync.load_csv(csv_files_to_load[stream], row_count[stream])
    row_count[stream] = 0
    primary_key_exists[stream] = {}
    csv_files_to_load[stream] = NamedTemporaryFile(mode='w+b')

def walk_schema_for_numeric_precision(schema):
    if isinstance(schema, list):
        for v in schema:
            walk_schema_for_numeric_precision(v)
    elif isinstance(schema, dict):
        if numeric_schema_with_precision(schema):
            def get_precision(key):
                v = abs(Decimal(schema.get(key, 1))).log10()
                if v < 0:
                    return round(math.floor(v))
                return round(math.ceil(v))
            scale = -1 * get_precision('multipleOf')
            digits = max(get_precision('minimum'), get_precision('maximum'))
            precision = digits + scale
            if decimal.getcontext().prec < precision:
                logger.debug('Setting decimal precision to {}'.format(precision))
                decimal.getcontext().prec = precision
        else:
            for v in schema.values():
                walk_schema_for_numeric_precision(v)

def validate_config():
    pass

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)
        
def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    csv_files_to_load = {}
    row_count = {}
    stream_to_sync = {}
    primary_key_exists = {}
    batch_size = config['batch_size'] if 'batch_size' in config else 100000
    
    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            # Get schema for this record's stream
            stream = o['stream']

            # Validate record
            validators[stream].validate(float_to_decimal(o['record']))

            sync = stream_to_sync[stream]

            primary_key_string = sync.record_primary_key_string(o['record'])
            if stream not in primary_key_exists:
                primary_key_exists[stream] = {}
            if primary_key_string and primary_key_string in primary_key_exists[stream]:
                flush_records(o, csv_files_to_load, row_count, primary_key_exists, sync)

            csv_line = sync.record_to_csv_line(o['record'])
            csv_files_to_load[o['stream']].write(bytes(csv_line + '\n', 'UTF-8'))
            row_count[o['stream']] += 1
            if primary_key_string:
                primary_key_exists[stream][primary_key_string] = True

            if row_count[o['stream']] >= batch_size:
                flush_records(o, csv_files_to_load, row_count, primary_key_exists, sync)


            state = None
        elif t == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            schema = float_to_decimal(o['schema'])
            if 'properties' not in schema:
                logger.debug(f"Schema for stream '{stream}' misses properties. Waiting for properly-formed schema.")
                continue
            schemas[stream] = o
            walk_schema_for_numeric_precision(schema)
            validators[stream] = Draft4Validator(schema, format_checker=FormatChecker())
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o['key_properties']

            stream_to_sync[stream] = DbSync(config, o)
            stream_to_sync[stream].create_schema_if_not_exists()
            stream_to_sync[stream].sync_table()
            row_count[stream] = 0
            csv_files_to_load[stream] = NamedTemporaryFile(mode='w+b')        
        elif t == 'ACTIVATE_VERSION':
            pass
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))
    
    for (stream_name, count) in row_count.items():
        if count > 0:
            stream_to_sync[stream_name].load_csv(csv_files_to_load[stream_name], count)

    return state


def send_usage_stats():
    try:
        version = pkg_resources.get_distribution('target-mysql').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-mysql',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()
    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    # Todo: Need to validate Config values

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(config, input)
        
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
