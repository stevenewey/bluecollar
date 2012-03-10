#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    BlueCollar

    Gevent.Websocket server
    Use with gunicorn:
     gunicorn -b 127.0.0.1:8003 \
        -k "geventwebsocket.gunicorn.workers.GeventWebSocketWorker" \
        bluecollar.websocket:application

"""

# standard library
import os
import json
import logging
import sys
import uuid

# third party modules
import gevent
import gevent.monkey
gevent.monkey.patch_all()
from gevent.pywsgi import WSGIServer
import geventwebsocket

# bluecollar things
import bluecollar.worker as bcenv
from bluecollar.http import application as http_fallback
from bluecollar.rest import application as rest_fallback

# settings from env
_WS_HOST = os.environ.get('BC_WS_HOST', '0.0.0.0')
try:
    _WS_PORT = abs(int(os.environ.get('BC_WS_PORT', 8003)))
    _REQUEST_TIMEOUT = abs(int(os.environ.get('BC_WS_TIMEOUT', 300)))
except ValueError, err:
    logging.error(err)
    sys.exit(1)
_WS_FALLBACK = os.environ.get('BC_WS_FALLBACK')
_REPLY_PREFIX = os.environ.get('BC_WS_REPLY_PREFIX', 'bc')

def application(env, start_response):
    """WSGI WS Application"""
    websocket = env.get('wsgi.websocket')
    reply_channel = '%s_%s' % (_REPLY_PREFIX, uuid.uuid1().hex)
    if websocket is None:
        if _WS_FALLBACK == 'http':
            return http_fallback(env, start_response)
        elif _WS_FALLBACK == 'rest':
            return rest_fallback(env, start_response)
        start_response('400 Bad Request', [])
        return ['WebSocket connection is expected here.']

    try:
        while True:
            message = websocket.receive()
            if message is None:
                break
            try:
                message = json.loads(message)
            except ValueError:
                websocket.send(json.dumps('Unable to JSON decode request.'))
                continue
            if type(message) is dict:
                if type(message.get('subscribe')) is list:
                    pass
                else:
                    message['reply_channel'] = reply_channel
                    bcenv.REDIS.rpush(bcenv.WORKER_QUEUE,
                            json.dumps(message))
                    response = bcenv.REDIS.blpop(reply_channel,
                            _REQUEST_TIMEOUT)
                    if not response:
                        websocket.send(json.dumps('Requested timed out.'))
                        continue
                    websocket.send(response[1])
        websocket.close()
    except geventwebsocket.WebSocketError, message:
        logging.error('%s: %s', message.__class__.__name__, message)


if __name__ == '__main__':
    logging.info('BlueCollar WebSocket Server at %s:%d', _WS_HOST, _WS_PORT)
    WSGIServer(
            (_WS_HOST, _WS_PORT),
            application,
            handler_class=geventwebsocket.WebSocketHandler).serve_forever()
