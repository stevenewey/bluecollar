#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    BlueCollar

    Gevent WSGI Server for interfacing a BlueCollar worker with HTTP
    Use with gunicorn:
     gunicorn -b 127.0.0.1:8001 -k gevent bluecollar.http:application
"""

# builtins
import os
import sys
import json
import logging
import urlparse
import uuid

# third party modules
import gevent
import gevent.monkey
gevent.monkey.patch_socket()
import redis
from gevent.pywsgi import WSGIServer

# bluecollar modules
import bluecollar.worker as bcenv

# where shall we bind
_HTTP_HOST = os.environ.get('BC_HTTP_HOST', '0.0.0.0')
try:
    _HTTP_PORT = abs(int(os.environ.get('BC_HTTP_PORT', 8001)))
    _REQUEST_TIMEOUT = abs(int(os.environ.get('BC_HTTP_TIMEOUT', 300)))
except ValueError, message:
    logging.error(message)
    sys.exit(1)
_REQUEST_PREFIX = os.environ.get('BC_HTTP_PREFIX', '/')
_REPLY_PREFIX = os.environ.get('BC_HTTP_REPLY_PREFIX', 'bc')

def application(env, start_response):
    """WSGI application"""
    error = None
    reply_channel = '%s_%s' % (_REPLY_PREFIX, uuid.uuid1().hex)
    if env['REQUEST_METHOD'] == 'GET':
        # GET requests, work with path and args
        if env['PATH_INFO'].startswith(_REQUEST_PREFIX):
            request = env['PATH_INFO'][len(_REQUEST_PREFIX):].split('/')
            kwargs = urlparse.parse_qs(env['QUERY_STRING'])
            bcenv.REDIS.rpush(bcenv.WORKER_QUEUE, json.dumps({
                'method' : request[0],
                'args' : request[1:],
                'kwargs' : kwargs,
                'reply_channel' : reply_channel,
                }))
            response = bcenv.REDIS.blpop(reply_channel, _REQUEST_TIMEOUT)
            if not response:
                error = 'Timed out waiting for response.'
        else:
            error = 'Expected prefix %s not found in request path.' % (
                    _REQUEST_PREFIX)
    elif env['REQUEST_METHOD'] == 'POST':
        # POST requests, expect JSON data we can just pass on with a reply chan
        try:
            request = json.loads(env['wsgi.input'].read())
        except ValueError:
            error = 'Unable to parse JSON data in POST.'
        if not error:
            if type(request) != dict:
                error = 'Expected dict in POST data, received %s' % type(request)
            request['reply_channel'] = reply_channel
            bcenv.REDIS.rpush(bcenv.WORKER_QUEUE, json.dumps(request))
            response = bcenv.REDIS.blpop(reply_channel, _REQUEST_TIMEOUT)
            if not response:
                error = 'Timed out waiting for response.'
    else:
        start_response('501 Not Implemented', [('Content-Type', 'text/plain')])
        return ['501: Method not implemented. Only GET/POST are expected.']
    if error:
        start_response('500 Internal Server Error', [('Content-Type',
            'text/plain')])
        return ['500: %s' % error]
    start_response('200 OK', [('Content-Type','application/json')])
    return [response[1]]

if __name__ == '__main__':
    logging.info('BlueCollar HTTP Server at %s:%d', _HTTP_HOST, _HTTP_PORT)
    WSGIServer((_HTTP_HOST, _HTTP_PORT), application).serve_forever()

