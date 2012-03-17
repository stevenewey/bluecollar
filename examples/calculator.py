# -*- coding: utf-8 -*-
"""
    BlueCollar examples

    Simple calculator
"""

# python standard lib
import logging

# third party modules
import gevent

# import the Cacheble prototype
from bluecollar.prototype import Cacheable
from bluecollar.auth import authenticate

class Calculator(Cacheable):
    """Simple calculator functions"""

    def __init__(self):
        """We'll keep the last result around"""
        self.last_result = 0

    @authenticate(stop_on_fail=True)
    def add(self, op1, op2=None, auth=None):
        """Simple addition of two operators"""
        if type(op1) is list:
            op1 = op1[0]
        if type(op2) is list:
            op2 = op2[0]
        op2 = int(op2) if op2 is not None else self.last_result
        self.last_result = int(op1) + op2
        return self.last_result

    def http_get(self, op1, op2=None):
        """Simple addition of two operators"""
        if type(op1) is list:
            op1 = op1[0]
        if type(op2) is list:
            op2 = op2[0]
        op2 = int(op2) if op2 is not None else self.last_result
        self.last_result = int(op1) + op2
        return self.last_result

    def subtract(self, op1, op2=None):
        """Simple subtraction of two operators"""
        if not op2:
            self.last_result = self.last_result - int(op1)
        else:
            self.last_result = int(op1) - int(op2)
        return self.last_result

    def one_minute(self):
        gevent.sleep(60)

