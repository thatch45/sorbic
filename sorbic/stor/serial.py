'''
Construct the classes for serialization
'''

# Import python libs
import json

# Import third party libs
import msgpack


class Serial(object):
    '''
    Encapsulate the serialization routines
    '''
    def __init__(self, default='msgpack'):
        self.default = default

    def msgpack_dump(self, data):
        return msgpack.dumps(data)

    def msgpack_load(self, raw):
        return msgpack.loads(raw)

    def json_dump(self, data):
        return json.dumps(data)

    def json_load(self, raw):
        return json.loads(raw)
