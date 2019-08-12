#!/usr/bin/env python

from os import path

from setuptools import setup, find_packages

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='target-redshift',
    url='https://github.com/datamill-co/target-redshift',
    author='datamill',
    version="0.0.8",
    description='Singer.io target for loading data into redshift',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_redshift'],
    install_requires=[
        'boto3==1.9.205',
        'singer-target-postgres==0.1.9',
        'urllib3==1.25.3'
    ],
    setup_requires=[
        "pytest-runner"
    ],
    tests_require=[
        "chance==0.110",
        "Faker==1.0.8",
        "pytest==4.6.5"
    ],
    entry_points='''
      [console_scripts]
      target-redshift=target_redshift:cli
    ''',
    packages=find_packages()
)
