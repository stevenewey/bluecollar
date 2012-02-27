DATA = [
        {'id' : 1, 'name' : 'Thing A', },
        {'id' : 2, 'name' : 'Thing B', },
        {'id' : 3, 'name' : 'Thing C', },
    ]

class Resource(object):
    def http_get(self, *args, **kwargs):
        if args:
            item = int(args[0])
            return Item(item).http_get(*args, **kwargs)
        else:
            return [item['id'] for item in DATA]

class Item(object):
    def __init__(self, item_id):
        self.record = DATA[item_id]

    def http_get(self, *args, **kwargs):
        return self.record

