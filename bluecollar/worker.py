#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    BlueCollar

    Worker process
    Wraps Python classes in a queue-assisted request handler

"""

# builtin modules
import sys
import os
import logging
import time
import argparse

# thid party modules
import redis

# bluecollor modules


# logging level and format
_LOG_FORMAT = '%(asctime)s\tPID:%(process)d\t%(filename)s\t%(levelname)s\t\
%(relativeCreated)dms\t%(message)s'
if os.environ.get('DEBUG'):
    logging.basicConfig(level=logging.DEBUG, format=_LOG_FORMAT)
else:
    logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)

# redis connection
_REDIS_HOST = os.environ.get('REDISHOST', 'localhost')
try:
    _REDIS_PORT = abs(int(os.environ.get('REDISPORT', 6379)))
    _REDIS_DB = int(os.environ.get('REDISDB', 0))
    if _REDIS_DB < 0 or _REDIS_DB > 15:
        raise ValueError("Redis DBs must be 0-15.")
except ValueError, message:
    logging.error(message)
    sys.exit(1)
REDIS = redis.Redis(_REDIS_HOST, _REDIS_PORT, _REDIS_DB)

# check we've got a module to argue with
arg_parser = argparse.ArgumentParser(description='BlueCollar worker process')
arg_parser.add_argument('module', metavar='module_name', type=str,
        nargs=1, help='Module to be exposed via Redis API')
args = arg_parser.parse_args()
try:
    MODULE = __import__(args.module[0])
except ImportError:
    logging.error('Unable to import module %s', args.module[0])
    sys.exit(1)


