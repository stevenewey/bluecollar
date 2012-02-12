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
import json
import signal

# thid party modules
import redis
import gevent

# bluecollar modules

# our PID will be checked in redis to see if we should die
_PID = os.getpid()

# logging level and format
_LOG_FORMAT = '%(asctime)s\tPID:%(process)d\t%(filename)s\t%(levelname)s\t\
%(relativeCreated)dms\t%(message)s'
if os.environ.get('DEBUG'):
    logging.basicConfig(level=logging.DEBUG, format=_LOG_FORMAT)
else:
    logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)

# check we've got a module to argue with
ARGPARSER = argparse.ArgumentParser(description='BlueCollar worker process')
ARGPARSER.add_argument('module', metavar='module_name', type=str,
        nargs=1, help='Module to be exposed via Redis API')
ARGS = ARGPARSER.parse_args()
try:
    MODULE = __import__(ARGS.module[0])
except ImportError:
    logging.error('Unable to import module %s', ARGS.module[0])
    sys.exit(1)

# redis connection
_REDIS_HOST = os.environ.get('BC_REDISHOST', 'localhost')
try:
    _REDIS_PORT = abs(int(os.environ.get('BC_REDISPORT', 6379)))
    _REDIS_DB = int(os.environ.get('BC_REDISDB', 0))
    if _REDIS_DB < 0 or _REDIS_DB > 15:
        raise ValueError("Redis DBs must be 0-15.")
except ValueError, message:
    logging.error(message)
    sys.exit(1)
_REDIS = redis.Redis(_REDIS_HOST, _REDIS_PORT, _REDIS_DB)
_WORKER_QUEUE = os.environ.get('BC_QUEUE', 'list_bcqueue')
_WORKER_LIST = os.environ.get('BC_WORKERLIST', 'list_bcworkers')

# instance cache for reusable classes
_INST_CACHE = {}
# cache of executable things
_EXEC_CACHE = {}

def clean_exit(*args):
    """Clean up on exit"""
    logging.info('User exited')
    _REDIS.srem(_WORKER_LIST, _PID)
    sys.exit(0)
signal.signal(signal.SIGTERM, clean_exit)

# main loop
if __name__ == '__main__':
    # big old exception catcher for redis failing
    try:
        # add worker to list
        _REDIS.sadd(_WORKER_LIST, _PID)

        while True:
            if not _REDIS.sismember(_WORKER_LIST, _PID):
                # we're no longer welcome, break out of the main loop
                logging.info('PID removed from worker list, exiting.')
                break

            # grab the next request from the worker queue, or wait
            REQUEST = _REDIS.blpop(_WORKER_QUEUE, 15)
            if not REQUEST:
                # timeout waiting for request, lets us run the loop
                # again and check if we should still be here
                continue

            # request should be JSON
            try:
                REQUEST = json.loads(REQUEST[1])
            except ValueError:
                logging.error('Invalid JSON for request: %s', REQUEST[1])
                continue

            # request should be a dict and have a request key with list val
            if (type(REQUEST) is not dict or
                    not REQUEST.has_key('request') or
                    type(REQUEST['request']) is not list or
                    len(REQUEST['request']) is 0 or
                    type(REQUEST['request'][0]) not in [unicode, str]):
                logging.error('Missing or invalid request: %s', REQUEST)
                continue

            logging.info('Request: %s', REQUEST)

    except redis.exceptions.ConnectionError, message:
        # redis isn't there or went away
        # wait 5 secs before exit to not upset upstart
        logging.error('Redis unavailable: %s', message)
        time.sleep(5)
        sys.exit(1)

    except KeyboardInterrupt:
        # user interrupted
        clean_exit()

