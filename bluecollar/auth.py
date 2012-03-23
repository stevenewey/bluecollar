# -*- coding: utf-8 -*-
"""
    BlueCollar

    Authentication helpers

"""

# stdlib

# third party modules

# bluecollar modules

class authenticate(object):

    def __init__(self, stop_on_fail=True):
        self._stop_on_fail = stop_on_fail

    def __call__(self, fn):
        def wrapped(*args, **kwargs):
            auth = self.authenticate(kwargs)
            if self._stop_on_fail and auth.get('authenticated') is not True:
                return 'Authentication failed.'
            kwargs['auth'] = auth
            return fn(*args, **kwargs)
        return wrapped

    def authenticate(self, kwargs):
        return {
            'authenticated': False,
            'user' : {},
            }
