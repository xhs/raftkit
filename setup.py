#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name='raftkit',
    version='0.1.0',
    description='raftkit',
    author='xhs',
    author_email='chef@dark.kitchen',
    url='https://github.com/xhs/raftkit',
    packages=['raftkit'],
    install_requires=['structlog', 'docopt'],
    classifiers=[
        'Programming Language :: Python :: 3'
    ]
)
