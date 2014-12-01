# -*- coding: utf-8 -*-
'''
Storage managers
'''
# Import python libs
import os
import io
import json
# Import Third Party Libs
import msgpack


class Stor(object):
    '''
    Default storage mapping system
    '''
    def __init__(self, root, serial):
        self.root = root
        self.serial = serial
        self.stores = {}

    def msgpack_dump(self, data):
        '''
        Read in data serailizing it into msgpack
        '''
        return msgpack.dumps(data)

    def msgpack_load(self, raw_data):
        '''
        Read out data deserializing it from msgpack
        '''
        return msgpack.loads(raw_data)

    def json_dump(self, data):
        '''
        Read in data serializing it into json
        '''
        return json.dumps(data)

    def json_load(self, raw_data):
        '''
        Read out data deserializing it from json
        '''
        return json.loads(raw_data)

    def pickle_load(self, raw_data):
        '''
        Handle the pickle storage backend
        NOTICE - USING PICKLE IS NOT SECURE! REMOTE USERS CAN INJECT
        COMMANDS, THIS WILL STAY DISABLED UNTIL IT CAN BE DISABLED BY DEFAULT
        '''
        raise NotImplementedError
        # return pickle.loads(raw_data)

    def pickle_dump(self, data):
        '''
        Handle the pickle storage backend
        NOTICE - USING PICKLE IS NOT SECURE! REMOTE USERS CAN INJECT
        COMMANDS, THIS WILL STAY DISABLED UNTIL IT CAN BE DISABLED BY DEFAULT
        '''
        raise NotImplementedError
        # return pickle.dumps(data)

    def fn_from_table(self, table_entry):
        '''
        Get the storage file, open it if needed and add to the stores
        '''
        sfn = os.path.join(
            os.path.dirname(table_entry['tfn']),
            'sorbic_stor_{0}'.format(table_entry['num']))
        if sfn in self.stores:
            return self.stores[sfn]
        try:
            fp_ = io.open(sfn, 'r+b')
        except (OSError, IOError):
            fp_ = io.open(sfn, 'w+b')
        stor = {'fp': fp_}
        self.stores[sfn] = stor
        return stor

    def write(self, table_entry, data, serial=None):
        '''
        Write the data to the storage file
        '''
        stor = self.fn_from_table(table_entry)
        serial = serial if serial else self.serial
        serial_fun = getattr(self, '{0}_dump'.format(serial))
        serial_data = serial_fun(data)
        stor['fp'].seek(0, 2)
        start = stor['fp'].tell()
        stor['fp'].write(serial_data)
        return start, len(serial_data)

    def read(self, table_entry, start, size, serial=None):
        '''
        Read in the data
        '''
        stor = self.fn_from_table(table_entry)
        stor['fp'].seek(start)
        raw = stor['fp'].read(size)
        serial = serial if serial else self.serial
        serial_fun = getattr(self, '{0}_load'.format(serial))
        return serial_fun(raw)
