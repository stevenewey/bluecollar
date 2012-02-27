# -*- coding: utf-8 -*-
"""
    BlueCollar

    Prototype definition
    Inherit from our prototype class
"""

# any imports?


class Cacheable(object):
    """Prototype class for a cacheable class"""
    pass

class Resource(object):
    """Prototype REST-served resource"""
    def http_get(self, args=None):
        """GET: Return a list of all instances"""
        args = args or []

    def get(self, reference, args=None):
        """Internal use: Retrieve instance located by reference"""
        args = args or []
        item = Item(reference)
        if item:
            return item.http_get(args)

    def http_post(self, args=None):
        """POST: Create new item and return instance ref"""
        args = args or []

    def http_put(self, args=None):
        """PUT: (update) fails here"""
        args = args or []

    def http_delete(self, args=None):
        """DELETE: Delete based on args (dangerous?)"""
        args = args or []

class Item(object):
    """Prototype REST-served resource item"""
    def __init__(self, reference=None):
        """Load item from DB (or empty item if no reference)"""
        pass

    def http_get(self, args=None):
        """GET: Read data from item"""
        args = args or []

    def http_put(self, args=None):
        """PUT: Write data to item"""
        args = args or []

    def http_post(self, args=None):
        """POST: (create) fails here"""
        args = args or []

    def http_delete(self, args=None):
        """DELETE: delete instance"""
        args = args or []

