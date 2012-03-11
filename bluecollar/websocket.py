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

# third party modules
import gevent
import gevent.monkey
gevent.monkey.patch_all()
from gevent.pywsgi import WSGIServer
import geventwebsocket
import mmstats

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

    def json_helper(self, data):
        return data

    def authenticate_subscribe(self, websocket, client_id, channels):
        return True

    def piper(self, websocket, client_id):
        logging.debug('Subcribe pipe started for %s', client_id)
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
                pubsub = bcenv.REDIS.pubsub()
                self.clients[client_id] = {
                        'pubsub' : pubsub,
                        'channels' : [],
                        'worker' : None,
                        }
                self.clients[client_id]['worker'] = gevent.Greenlet.spawn(
                        self.piper, websocket, client_id)
            pubsub.subscribe(channels)
            self.clients[client_id]['channels'] = pubsub.channels
            logging.debug('Client %s now subscribed to %s',
                    client_id, pubsub.channels)

    def unsubscribe(self, websocket, client_id, channels):
        client = self.clients.get(client_id)
        if not client:
            logging.error('Non-existent client tried to unsubscribe: %s',
                    client_id)
            return False
        client['pubsub'].unsubscribe(channels)
        client['channels'] = client['pubsub'].channels
        logging.debug('Client %s now subscribed to %s',
                client_id, client['pubsub'].channels)
        return True

    def __call__(self, env, start_response):
        """WSGI WS Application"""
        websocket = env.get('wsgi.websocket')
        if websocket is None:
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

