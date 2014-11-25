# -*- coding: utf-8 -*-
'''
Utils
'''

# Import python libs
import os
import time
import struct
import hashlib
import binascii
import datetime

# create a standard epoch so all platforms will count revs from
# a standard epoch of jan 1 2014
STD_EPOCH = time.mktime(datetime.datetime(2014, 1, 1).timetuple())


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


def gen_rev():
    '''
    Return a revision based on timestamp
    '''
    r_time = time.time() - STD_EPOCH
    return struct.pack('>Q', r_time * 1000000)
