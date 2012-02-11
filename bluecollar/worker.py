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

# thid party modules
import redis

# bluecollor modules

# useful things to know
_PID = os.getpid()
_REDIS_HOST = os.environ.get('REDISHOST', 'localhost')
try:
    _REDIS_PORT = abs(int(os.environ.get('REDISPORT', 6379)))
except ValueError:
    logging.error('REDISPORT environment variable must be an positive integer.')
    sys.exit(1)
try:
    _REDIS_DB = int(os.environ.get('REDISDB', 0))
    if _REDIS_DB < 0 or _REDIS_DB > 15:
        raise ValueError("Redis DBs must be 0-15.")
except ValueError:
    logging.error('REDISDB environment variable must be an integer and 0-15.')

class Worker(object):
    """
    Worker objects serve things

    """

    def __init__(self, module):
        """Constructor connects to Redis, establishes module is avavilable"""
        self.redis = redis.Redis(_REDIS_HOST, _REDIS_PORT, _REDIS_DB)


