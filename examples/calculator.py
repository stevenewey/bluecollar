# -*- coding: utf-8 -*-
"""
    BlueCollar examples

    Simple calculator
"""
# third party modules
import gevent

# import the Cacheble prototype
from bluecollar.prototype import Cacheable

class Calculator(Cacheable):
    """Simple calculator functions"""

    def __init__(self):
        """We'll keep the last result around"""
        self.last_result = 0

    def add(self, op1, op2=None):
        """Simple addition of two operators"""
        op2 = op2 if op2 is not None else self.last_result
        self.last_result = op1 + op2
        return self.last_result

    def subtract(self, op1, op2=None):
        """Simple subtraction of two operators"""
        if not op2:
            self.last_result = self.last_result - op1
        else:
            self.last_result = op1 - op2
        return self.last_result

    def one_minute(self):
        gevent.sleep(60)

