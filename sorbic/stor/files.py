# -*- coding: utf-8 -*-
'''
Read and Write files to disk and return the relative path data for the index
'''
# Import python libs
import os
import io
import binascii
# Import sorbic libs
import sorbic.utils.rand


def _file_loc(files_dir):
    while True:
        fn_ = os.path.join(files_dir, sorbic.utils.rand.rand_hex_str(24))
        if not os.path.exists(fn_):
            return fn_


def write(table_entry, data, serial=None):
    '''
    Write the file entry and return the needed metadata to find it
    '''
    if serial:
        serial = None
    files_dir = os.path.join(
            os.path.dirname(table_entry['tfn']),
            '.files_{0}'.format(table_entry['num']))
    if not os.path.isdir(files_dir):
        os.makedirs(files_dir)
    fn_ = _file_loc(files_dir)
    with io.open(fn_, 'wb+') as fp_:
        fp_.write(data)
    ret = {'path': fn_,
           'crc': binascii.crc32(data) & 0xffffffff}
    return ret


def read(entries, serial=None, **kwargs):
    '''
    Return the file data
    '''
    fn_ = entries['data']['path']
    try:
        with io.open(fn_, 'rb') as fp_:
            return fp_.read()
    except (OSError, IOError):
        return ''
