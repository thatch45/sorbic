# -*- coding: utf-8 -*-
'''
Storage using msgpack for serialization
'''
# Import third party libs
import msgpack


class Mpack(object):
    '''
    msgpack!
    '''
    def __init__(self, root):
        self.root = root

    def dump(self, data):
        '''
        prep the data for storage
        '''
        return msgpack.dumps(data)

    def load(self, raw_data):
        '''
        load data into serialized form
        '''
        return msgpack.loads(raw_data)
