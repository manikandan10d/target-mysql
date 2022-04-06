#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-mysql",
    version="0.1.0",
    description="Singer.io target for loading data to MySQL",
    author="Manikandan Balakrishnan",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_mysql"],
    install_requires=[
        "singer-python>=5.0.12",
        "PyMySQL",
        "inflection",
        "pandas"
    ],
    entry_points="""
    [console_scripts]
    target-mysql=target_mysql:main
    """,
    packages=["target_mysql"],
    package_data = {},
    include_package_data=True,
)
