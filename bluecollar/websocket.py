#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    BlueCollar

    Gevent.Websocket server
    Use with gunicorn:
     gunicorn -b 127.0.0.1:8003 \
        -k "geventwebsocket.gunicorn.workers.GeventWebSocketWorker" \
        bluecollar.websocket:WebSocketApplication

"""

# standard library
import os
import json
import logging
import sys
import uuid
import urlparse

# third party modules
import gevent
import gevent.monkey
gevent.monkey.patch_all()
from gevent.pywsgi import WSGIServer
import geventwebsocket
import mmstats
import redis

# bluecollar things
import bluecollar.worker as bcenv
from bluecollar.http import application as http_fallback
from bluecollar.rest import application as rest_fallback

# settings from env
_WS_HOST = os.environ.get('BC_WS_HOST', '0.0.0.0')
try:
    _WS_PORT = abs(int(os.environ.get('BC_WS_PORT', 8003)))
    _REQUEST_TIMEOUT = abs(int(os.environ.get('BC_WS_TIMEOUT', 300)))
    _WS_REDISPORT = abs(int(os.environ.get('BC_WS_REDISPORT',
        bcenv.REDIS_PORT)))
    _WS_REDISDB = abs(int(os.environ.get('BC_WS_REDISDB', bcenv.REDIS_DB)))
except ValueError, err:
    logging.error(err)
    sys.exit(1)
_WS_FALLBACK = os.environ.get('BC_WS_FALLBACK')
_REPLY_PREFIX = os.environ.get('BC_WS_REPLY_PREFIX', 'bc')
_WS_REDISHOST = os.environ.get('BC_WS_REDISHOST', bcenv.REDIS_HOST)
_WS_SKIP_LONGPOLLING = os.environ.get('BC_WS_SKIP_LONGPOLLING', False)

class WebSocketStats(mmstats.MmStats):
    connections_handled = mmstats.CounterField(label='connections_handled')
    connections_open = mmstats.UInt64Field(label='connections_open')
    pubsub_connections = mmstats.UInt64Field(label='pubsub_connections')
    pubsub_events = mmstats.CounterField(label='pubsub_events')
WS_STATS = WebSocketStats(label_prefix='me.s-n.bluecollar.websocket.')

class WebSocketApplication(object):
    """
    BlueCollar Generic web socket handler process
    Subclass to add pub/sub handling functionality

    """

    def __init__(self):
        self.clients = {}
        self._REDIS = redis.Redis(_WS_REDISHOST, _WS_REDISPORT, _WS_REDISDB)

    def json_helper(self, data):
        return data

    def authenticate_subscribe(self, websocket, client_id, channels):
        return True

    def authenticate_subscribe_xhr(self, start_response, kwargs, channels):
        return True

    def piper(self, websocket, client_id):
        logging.debug('Subscribe pipe started for %s', client_id)
        pubsub = self.clients[client_id]['pubsub']
        WS_STATS.pubsub_connections += 1
        while client_id in self.clients:
            for message in pubsub.listen():
                logging.debug('Message for %s', client_id)
                WS_STATS.pubsub_events.inc()
                websocket.send(json.dumps(message, self.json_helper))
            gevent.sleep()
        logging.debug('Leaving pipe for %s', client_id)
        pubsub.reset()

    def subscribe(self, websocket, client_id, channels):
        if self.authenticate_subscribe(websocket, client_id, channels):
            if self.clients.get(client_id):
                pubsub = self.clients[client_id]['pubsub']
            else:
                logging.debug('New PubSub connection for %s', client_id)
                pubsub = self._REDIS.pubsub()
                self.clients[client_id] = {
                        'pubsub' : pubsub,
                        'channels' : [],
                        'worker' : None,
                        }
            if self.clients[client_id].get('worker'):
                # have to kill any existing greenlet to avoid socket error
                self.clients[client_id]['worker'].kill()
            pubsub.subscribe(channels)
            self.clients[client_id]['worker'] = gevent.Greenlet.spawn(
                    self.piper, websocket, client_id)
            self.clients[client_id]['channels'] = pubsub.channels
            logging.debug('Client %s now subscribed to %s',
                    client_id, pubsub.channels)

    def unsubscribe(self, websocket, client_id, channels):
        client = self.clients.get(client_id)
        if not client:
            logging.error('Non-existent client tried to unsubscribe: %s',
                    client_id)
            return False
        client['worker'].kill()
        if channels == []:
            del self.clients[client_id]
            return True
        client['pubsub'].unsubscribe(channels)
        client['worker'] = gevent.Greenlet.spawn(
                self.piper, websocket, client_id)
        client['channels'] = client['pubsub'].channels
        logging.debug('Client %s now subscribed to %s',
                client_id, client['pubsub'].channels)
        return True

    def xhr_long_polling(self, env, start_response):
        if env['REQUEST_METHOD'] == 'POST':
            try:
                kwargs = json.loads(env['wsgi.input'].read())
            except ValueError:
                start_response('400 Bad Request', [])
                return ['POST request must contain JSON data.']
        else:
            kwargs = urlparse.parse_qs(env['QUERY_STRING'])
        if not kwargs.get('subscribe'):
            start_response('400 Bad Request', [])
            return ['Long polling requests are only suppoered for PubSub.']
        client_id = '%s_%s' % (_REPLY_PREFIX, uuid.uuid1().hex)
        channels = kwargs['subscribe']
        if self.authenticate_subscribe_xhr(start_response, kwargs, channels):
            pubsub = bcenv.REDIS.pubsub()
            pubsub.subscribe(channels)
            logging.debug('Long polling client %s subscribed to %s',
                    client_id, channels)
            if kwargs.get('callback'):
                start_response('200 OK', [('Content-Type', 'text/javascript')])
            else:
                start_response('200 OK', [('Content-Type',
                    'application/json')])
            while True:
                for message in pubsub.listen():
                    if message['type'] != 'message':
                        continue
                    logging.debug('Message for %s', client_id)
                    if kwargs.get('callback'):
                        return ['%s(%s);' % (kwargs['callback'][0],
                            json.dumps(message, self.json_helper))]
                    else:
                        return [json.dumps(message, self.json_heler)]
            pubsub.reset()

    def __call__(self, env, start_response):
        """WSGI WS Application"""
        websocket = env.get('wsgi.websocket')
        if websocket is None:
            if not _WS_SKIP_LONGPOLLING and env['PATH_INFO'][-5:] == '/xhr/':
                return self.xhr_long_polling(env, start_response)
            if _WS_FALLBACK == 'http':
                return http_fallback(env, start_response)
            elif _WS_FALLBACK == 'rest':
                return rest_fallback(env, start_response)
            start_response('400 Bad Request', [])
            return ['WebSocket connection is expected here.']
        reply_channel = '%s_%s' % (_REPLY_PREFIX, uuid.uuid1().hex)
        logging.debug('Open socket for client %s', reply_channel)
        WS_STATS.connections_open += 1

        try:
            while True:
                message = websocket.receive()
                if message is None:
                    break
                try:
                    message = json.loads(message)
                except ValueError:
                    websocket.send(
                            json.dumps('Unable to JSON decode request.'))
                    continue
                if type(message) is dict:
                    if type(message.get('subscribe')) is list:
                        self.subscribe(websocket, reply_channel,
                                message['subscribe'])
                    elif type(message.get('unsubscribe')) is list:
                        self.unsubscribe(websocket, reply_channel,
                                message['unsubscribe'])
                    else:
                        message['reply_channel'] = reply_channel
                        bcenv.REDIS.rpush(bcenv.WORKER_QUEUE,
                                json.dumps(message))
                        response = bcenv.REDIS.blpop(reply_channel,
                                _REQUEST_TIMEOUT)
                        if not response:
                            websocket.send(
                                    json.dumps('Requested timed out.'))
                            continue
                        websocket.send(response[1])
            websocket.close()
            if self.clients.get(reply_channel):
                self.clients[reply_channel]['worker'].kill()
                del self.clients[reply_channel]
                WS_STATS.pubsub_connections -= 1
            WS_STATS.connections_open -= 1
            WS_STATS.connections_handled.inc()
            logging.debug('Closed socket for client %s', reply_channel)

        except geventwebsocket.WebSocketError, message:
            WS_STATS.connections_open -= 1
            WS_STATS.connections_handled.inc()
            logging.error('%s: %s', message.__class__.__name__, message)

def start_application(application_class):
    logging.info('BlueCollar WebSocket Server at %s:%d', _WS_HOST, _WS_PORT)
    WSGIServer(
            (_WS_HOST, _WS_PORT),
            application_class(),
            handler_class=geventwebsocket.WebSocketHandler).serve_forever()

if __name__ == '__main__':
    start_application(WebSocketApplication)

