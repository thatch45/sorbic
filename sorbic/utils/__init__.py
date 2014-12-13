# -*- coding: utf-8 -*-
'''
Utils
'''

# Import python libs
import os
import time
import random
import struct
import binascii
import datetime

# create a standard epoch so all platforms will count revs from
# a standard epoch of jan 1 2014
STD_EPOCH = time.mktime(datetime.datetime(2014, 1, 1).timetuple()) * 1000000
DEFAULT_TARGET_DELIM = ':'

def rand_hex_str(size):
    '''
    Return a random string of the passed size using hex encoding
    '''
    return binascii.hexlify(os.urandom(size/2))


def rand_raw_str(size):
    '''
    Return a raw byte string of the given size
    '''
    return os.urandom(size)


def gen_id():
    '''
    Return a revision based on timestamp
    '''
    r_time = time.time() * 1000000 - STD_EPOCH
    return struct.pack('>HQ', random.randint(0, 65535), r_time).encode('hex')


def traverse_dict_and_list(data, key, default=None, delimiter=DEFAULT_TARGET_DELIM):
    '''
    Traverse a dict or list using a colon-delimited (or otherwise delimited,
    using the 'delimiter' param) target string. The target 'foo:bar:0' will
    return data['foo']['bar'][0] if this value exists, and will otherwise
    return the dict in the default argument.
    Function will automatically determine the target type.
    The target 'foo:bar:0' will return data['foo']['bar'][0] if data like
    {'foo':{'bar':['baz']}} , if data like {'foo':{'bar':{'0':'baz'}}}
    then return data['foo']['bar']['0']
    '''
    for each in key.split(delimiter):
        if isinstance(data, list):
            try:
                idx = int(each)
            except ValueError:
                embed_match = False
                # Index was not numeric, lets look at any embedded dicts
                for embedded in (x for x in data if isinstance(x, dict)):
                    try:
                        data = embedded[each]
                        embed_match = True
                        break
                    except KeyError:
                        pass
                if not embed_match:
                    # No embedded dicts matched, return the default
                    return default
            else:
                try:
                    data = data[idx]
                except IndexError:
                    return default
        else:
            try:
                data = data[each]
            except (KeyError, TypeError):
                return default
    return data
