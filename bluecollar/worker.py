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
import prototype

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

def route_to_class_or_function(path, module):
    """Follow the dot-notation string to find the class or function"""
    # maintain route to module for submodule imports
    full_path = module.__name__
    items = path.split('.')
    if (hasattr(module, items[0]) and
            ((callable(getattr(module, items[0])) and
                len(items) == 1) or
            (type(getattr(module, items[0])) is type and
                len(items) == 2 and
                hasattr(getattr(module, items[0]), items[1]) and
                callable(getattr(getattr(module, items[0]), items[1]))))):
        # this module contains a callable function or class that contains
        # a callable method and there's no leftover items
        return getattr(module, items[0])
    if (hasattr(module, items[0]) and
            len(items) > 1 and
            hasattr(getattr(module, items[0]), items[1])):
        # we can find the next item down, so go look in there
        return route_to_class_or_function(
            '.'.join(items[1:]),
            getattr(module, items[0]))
    # try and load submodule and carry on
    if len(items) > 1:
        submodule = None
        try:
            submodule = __import__(
                    '%s.%s' % (full_path, items[0]),
                    fromlist=[str(items[1])])
        except ImportError:
            pass
        if submodule:
            return route_to_class_or_function(
                '.'.join(items[1:]),
                submodule)
    return False

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
            METHOD = REQUEST['request'][0]

            # attempt to resolve the requested function
            # keeping a cache of them along the way
            if _EXEC_CACHE.has_key(METHOD):
                EXECUTABLE = _EXEC_CACHE[METHOD]
            else:
                EXECUTABLE = route_to_class_or_function(
                    METHOD, MODULE)
                _EXEC_CACHE[METHOD] = EXECUTABLE
                if not EXECUTABLE:
                    logging.error('Failed to find class or function at %s',
                        METHOD)
                    continue

            # instantiate if we're dealing with a class
            if type(EXECUTABLE) is type:
                if issubclass(EXECUTABLE, prototype.Cacheable):
                    # inherits cacheable, we only need one
                    if not _INST_CACHE.has_key(METHOD):
                        # we don't have one yet, so make one
                        _INST_CACHE[METHOD] = EXECUTABLE()
                        logging.debug('New cacheable instance: %s',
                                _INST_CACHE[METHOD])
                    INSTANCE = _INST_CACHE[METHOD]
                else:
                    # instantiate a regular class every call
                    INSTANCE = EXECUTABLE()
                    logging.debug('New instance: %s', INSTANCE)
                # get the instance method we care about
                FUNC = getattr(INSTANCE, METHOD.split('.')[-1])
            else:
                # a normal function (outside a class)
                FUNC = EXECUTABLE

            logging.info('Requested class/func: %s', FUNC)

    except redis.exceptions.ConnectionError, message:
        # redis isn't there or went away
        # wait 5 secs before exit to not upset upstart
        logging.error('Redis unavailable: %s', message)
        time.sleep(5)
        sys.exit(1)

    except KeyboardInterrupt:
        # user interrupted
        clean_exit()

