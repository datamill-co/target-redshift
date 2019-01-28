#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='target-redshift',
    version="0.0.0.dev0",
    description='Singer.io target for loading data into redshift',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_redshift'],
    install_requires=[
        'boto3==1.9.79',
        'singer-target-postgres==0.1.3',
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
