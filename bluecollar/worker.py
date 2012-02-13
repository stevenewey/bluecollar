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
import json
import signal

# thid party modules
import redis
import gevent
import gevent.monkey

# bluecollar modules
from bluecollar import prototype

# our PID will be checked in redis to see if we should die
_PID = os.getpid()

# logging level and format
_LOG_FORMAT = '%(asctime)s\tPID:%(process)d\t%(filename)s\t%(levelname)s\t\
%(relativeCreated)dms\t%(message)s'
if os.environ.get('DEBUG'):
    logging.basicConfig(level=logging.DEBUG, format=_LOG_FORMAT)
else:
    logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)

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
# greenlet threads
_THREADS = []

def clean_exit(*args):
    """Clean up on exit"""
    logging.info('User exited: %s', args)
    _REDIS.srem(_WORKER_LIST, _PID)
    sys.exit(0)
signal.signal(signal.SIGTERM, clean_exit)

def route_to_class_or_function(path, module=None):
    """Follow the dot-notation string to find the class or function"""
    # maintain route to module for submodule imports
    if module:
        full_path = '%s.' % module.__name__
    else:
        full_path = ''
    items = path.split('.')
    if (module and
            hasattr(module, items[0]) and
            ((callable(getattr(module, items[0])) and
                len(items) == 1) or
            (type(getattr(module, items[0])) is type and
                len(items) == 2 and
                hasattr(getattr(module, items[0]), items[1]) and
                callable(getattr(getattr(module, items[0]), items[1]))))):
        # this module contains a callable function or class that contains
        # a callable method and there's no leftover items
        return getattr(module, items[0])
    if (module and
            hasattr(module, items[0]) and
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
                    '%s%s' % (full_path, items[0]),
                    fromlist=[str(items[1])])
        except ImportError:
            pass
        if submodule:
            return route_to_class_or_function(
                '.'.join(items[1:]),
                submodule)
    return False

def child(func, args, kwargs, reply_to):
    """Child function performs request function and handles response"""
    time_before = time.time()
    response = func(*args, **kwargs)
    time_after = time.time()
    if reply_to:
        pass
    logging.debug('%s executed in %s', func, time_after-time_before)


def main():
    # commence monkey patching of sockets for gevent
    gevent.monkey.patch_socket()
    # catch redis errors and keyboard interrupts
    try:
        _REDIS.sadd(_WORKER_LIST, _PID)
        # main loop
        while True:
            # if we're no longer welcome, break out of the main loop
            if not _REDIS.sismember(_WORKER_LIST, _PID):
                logging.info(
                    'Worker PID released, waiting for threads, then exiting.')
                for thread in _THREADS:
                    thread.join()
                break

            # clean up completed tasks
            for thread in _THREADS:
                if thread.ready():
                    _THREADS.remove(thread)
                    logging.debug('GC: %s', thread)

            # grab the next request from the worker queue, or wait
            request = _REDIS.blpop(_WORKER_QUEUE, 5)
            if not request:
                # timeout waiting for request, lets us run the loop
                # again and check if we should still be here
                continue

            # request should be JSON
            try:
                request = json.loads(request[1])
            except ValueError:
                logging.error('Invalid JSON for request: %s', request[1])
                continue

            # request should be a dict and have a request key with list val
            if (type(request) is not dict or
                    not request.has_key('method') or
                    type(request['method']) not in [unicode, str]):
                logging.error('Missing or invalid method: %s', request)
                continue
            method = request['method']

            # attempt to resolve the requested function
            # keeping a cache of them along the way
            if _EXEC_CACHE.has_key(method):
                executable = _EXEC_CACHE[method]
            else:
                executable = route_to_class_or_function(
                    method)
                _EXEC_CACHE[method] = executable
                if not executable:
                    logging.error('Failed to find class or function at %s',
                        method)
                    continue

            # instantiate if we're dealing with a class
            if type(executable) is type:
                if issubclass(executable, prototype.Cacheable):
                    # inherits cacheable, we only need one
                    if not _INST_CACHE.has_key(method):
                        # we don't have one yet, so make one
                        _INST_CACHE[method] = executable()
                        logging.debug('New cacheable instance: %s',
                                _INST_CACHE[method])
                    instance = _INST_CACHE[method]
                else:
                    # instantiate a regular class every call
                    instance = executable()
                    logging.debug('New instance: %s', instance)
                # get the instance method we care about
                func = getattr(instance, method.split('.')[-1])
            else:
                # a normal function (outside a class)
                func = executable

            # decode the arguments
            args = request.get('args', [])
            kwargs = request.get('kwargs', {})
            reply_to = request.get('reply_channel', None)

            # execute the function in a greenlet
            _THREADS.append(
                gevent.Greenlet.spawn(child, func, args, kwargs, reply_to))
            logging.debug('Requested class/func: %s %s %s', func, args, kwargs)


    except redis.exceptions.ConnectionError, message:
        # redis isn't there or went away
        # wait 5 secs before exit to not upset upstart
        logging.error('Redis unavailable: %s', message)
        time.sleep(5)
        sys.exit(1)
        # redis isn't there or went away
        # wait 5 secs before exit to not upset upstart
        logging.error('Redis unavailable: %s', message)
        time.sleep(5)
        sys.exit(1)

    except KeyboardInterrupt:
        # user interrupted
        clean_exit()

