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
    version="0.0.4",
    description='Singer.io target for loading data into redshift',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_redshift'],
    install_requires=[
        'boto3==1.9.79',
        'singer-target-postgres==0.1.5',
        'urllib3==1.24.1'
    ],
    setup_requires=[
        "pytest-runner"
    ],
    tests_require=[
        "chance==0.110",
        "Faker==1.0.1",
        "pytest==4.1.1"
    ],
    entry_points='''
      [console_scripts]
      target-redshift=target_redshift:cli
    ''',
    packages=find_packages()
)
