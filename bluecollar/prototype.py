# -*- coding: utf-8 -*-
"""
    BlueCollar

    Prototype definition
    Inherit from our prototype class
"""

# any imports?


class Prototype(object):
    """Prototype class for exposing your code via Redis"""

    def __init__(self):
        """
        Default options
        reusable: True

        """
        self._reusable = True

    @property
    def reusable(self):
        """reusable property getter"""
        return self._reusable

