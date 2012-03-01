#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    BlueCollar

    Gevent WSGI Server :: HTTP RESTful Interface to BlueCollar workers
    Use with gunicorn:
     gunicorn -b 127.0.0.1:8002 -k gevent bluecollar.rest:application

    WebSocket Gunicorn:
    gunicorn -k "geventwebsocket.gunicorn.workers.GeventWebSocketWorker" \
        bluecollar.rest:application
"""

# builtins
import os
import json
import logging
import urlparse
import sys
import uuid
import urllib

# thid party modules
import gevent
import gevent.monkey
gevent.monkey.patch_all()
from gevent.pywsgi import WSGIServer
from geventwebsocket.handler import WebSocketHandler

# bluecollar modules
import bluecollar.worker as bcenv

# where shall we bind
_REST_HOST = os.environ.get('BC_REST_HOST', '0.0.0.0')
try:
    _REST_PORT = abs(int(os.environ.get('BC_REST_PORT', 8002)))
    _REQUEST_TIMEOUT = abs(int(os.environ.get('BC_REST_TIMEOUT', 300)))
except ValueError, message:
    logging.error(message)
    sys.exit(1)
_REQUEST_PREFIX = os.environ.get('BC_REST_PREFIX', '/')
_REPLY_PREFIX = os.environ.get('BC_REST_REPLY_PREFIX', 'bc')
_ERROR_DOC_URL = os.environ.get('BC_REST_ERROR_DOC_URL')
_METHOD_CACHE = {}

def app_error(http_code, verbose_message, env, start_response):
    """Handle application errors"""
    error = {'message': verbose_message, }
    args = urlparse.parse_qs(env['QUERY_STRING'])
    if _ERROR_DOC_URL:
        # more_info URL constructed from envvar and error text
        error['more_info'] = '%s%s' % (_ERROR_DOC_URL,
                urllib.urlencode(verbose_message))
    if args.get('supress_response_codes'):
        error['response_code'] = http_code
        http_code = 200
    start_response('%d %s' % (http_code, verbose_message), [('Content-Type',
        'application/json')])
    return json.dumps(error)

def application(env, start_response):
    """WSGI REST application"""
    reply_channel = '%s_%s' % (_REPLY_PREFIX, uuid.uuid1().hex)
    kwargs = urlparse.parse_qs(env['QUERY_STRING'])
    http_method = kwargs.get('method') or env['REQUEST_METHOD'].lower()
    if not env['PATH_INFO'].startswith(_REQUEST_PREFIX):
        # doesn't look like this request is for us
        return app_error(404,
            'Invalid request path. Expected prefix %s' % _REQUEST_PREFIX,
            env, start_response)
    request = env['PATH_INFO'][len(_REQUEST_PREFIX):]
    elements = request.split('/')
    if elements[-1].rfind('.') > 0:
        # strip the file extension from the last element
        extension = elements[-1][elements[-1].rfind('.'):]
        elements[-1] = elements[-1][:-len(extension)]
        if extension != '.json':
            return app_error(406,
                'Unsupported content type %s.' % extension[1:],
                env, start_response)
    if http_method == 'options':
        start_response('200 OK', [
            ('Allow', 'GET, POST, PUT, DELETE, PATCH, OPTIONS'),
            ('Content-Length', 0),
            ('Access-Control-Allow-Headers', 'Origin, X-Requested-With'),
            ('Access-Control-Allow-Origin', '*')])
        return []
    # check cached methods, otherwise work forward through modules to find
    # a class with these methods
    method_path = None
    resource = None
    args = []
    for index, element in enumerate(elements):
        if method_path:
            method_path += '.%s' % element
        else:
            method_path = element
        if _METHOD_CACHE.has_key(method_path):
            if _METHOD_CACHE[method_path] is False:
                continue
            else:
                resource = method_path
                args = elements[_METHOD_CACHE[method_path]:]
                break
        bcenv.REDIS.rpush(bcenv.WORKER_QUEUE, json.dumps({
            'method' : '%s.http_%s' % (method_path, http_method),
            'no_exec' : True,
            'reply_channel' : reply_channel
            }))
        response = bcenv.REDIS.blpop(reply_channel, _REQUEST_TIMEOUT)
        if not response:
            return app_error(504,
                'Application did not respond in a timely fashion.',
                env, start_response)
        response = json.loads(response[1])
        if type(response) is dict and response.get('found'):
            resource = method_path
            args = elements[index+1:]
            _METHOD_CACHE[method_path] = index+1
            break
        else:
            _METHOD_CACHE[method_path] = False
    if not resource:
        return app_error(404,
            'No supported server method found.',
            env, start_response)
    bcenv.REDIS.rpush(bcenv.WORKER_QUEUE, json.dumps({
        'method' : '%s.http_%s' % (resource, http_method),
        'args' : args,
        'kwargs' : kwargs,
        'reply_channel' : reply_channel,
        }))
    response = bcenv.REDIS.blpop(reply_channel, _REQUEST_TIMEOUT)
    if not response:
        return app_error(504,
            'Application did not respond in a timely fashion.',
            env, start_response)
    start_response('200 OK', [('Content-Type', 'application/json'),
        ('Access-Control-Allow-Headers', '*'),
        ('Access-Control-Allow-Origin', '*')])
    return [response[1]]

if __name__ == '__main__':
    logging.info('BlueCollar REST Server at %s:%d', _REST_HOST, _REST_PORT)
    WSGIServer(
            (_REST_HOST, _REST_PORT),
            application,
            handler_class=WebSocketHandler).serve_forever()

