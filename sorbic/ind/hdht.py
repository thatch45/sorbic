# -*- coding: utf-8 -*-
'''
Higherarchical Distributed Hash Table index
'''
# Import python libs
import os
import io
import struct
import hashlib

# Import sorbic libs
import sorbic.utils

# Import Third Party Libs
import msgpack
import libnacl.blake

HEADER_DELIM = '_||_||_'


def _calc_pos(self, c_key, hash_limit, b_size, header_len):
    '''
    Calculate the hash position in the table file
    '''
    return (abs(hash(c_key) & hash_limit) * b_size) + header_len


class HDHT(object):
    '''
    The main index, the Higherarchical Distributed Hash Table
    '''
    def __init__(
            self,
            root,
            key_delim='/',
            hash_limit=0xffff,
            key_hash='blake',
            fmt='>KsQ',
            fmt_map=None,
            header_len=1024):
        if fmt_map is None:
            self.fmt_map = ('key', 'prev')
        else:
            self.fmt_map = fmt_map
        self.root = root
        self.key_delim = key_delim
        self.hash_limit = hash_limit
        self.key_hash = key_hash
        self.header_len = header_len
        self.crypt_func = self.__crypt_func()
        self.key_size = self.__gen_key_size()
        self.fmt = fmt.replace('K', str(self.key_size))
        self.bucket_size = self.__gen_bucket_size()
        self.tables = {}

    def __crypt_func(self):
        '''
        Return the function to use to crypt hash index keys
        '''
        if self.key_hash.startswith('blake'):
            return libnacl.blake.blake2b
        return getattr(hashlib, self.hash_key)

    def __gen_key_size(self):
        '''
        Return the length of the crypt_key
        '''
        return len(self.raw_crypt_key('sorbic is awesome'))

    def __gen_bucket_size(self):
        '''
        Calculate the size of the index buckets
        '''
        return len(struct.pack(self.fmt, '0' * self.key_size, 1))

    def raw_crypt_key(self, key):
        '''
        Return the crypted key
        '''
        return self.crypt_func(key.lstrip(self.key_delim)).digest()

    def entry_root(self, key):
        '''
        Return the root directory to be used for the entry
        '''
        key = key.strip(self.key_delim)
        root = key[:key.rfind(self.key_delim)].replace(self.key_delim, os.sep)
        os.path.join(self.root, root)

    def _open_hash_table(self, fn_):
        '''
        Return the header data for the table at the given location, open if
        needed
        '''
        if fn_ in self.tables:
            return self.tables[fn_]
        if not os.path.isfile(fn_):
            raise IOError()
        fp_ = io.open(fn_, 'r+b')
        header = {'fp': fp_}
        raw_head = ''
        while True:
            raw_read = fp_.read(self.header_len)
            if not raw_read:
                raise ValueError('Hit the end of the index file with no header!')
            raw_head += raw_read
            if HEADER_DELIM in raw_head:
                header.update(
                        msgpack.loads(
                            raw_head[:raw_head.find(HEADER_DELIM)]
                            )
                        )
                self.tables[fn_] = header
                return header

    def get_hash_table(self, fn_):
        '''
        Create a new hash table at the given location
        '''
        if os.path.exists(fn_):
            return self._open_hash_table(fn_)
        dirname = os.path.dirname(fn_)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        header = {
                'hash': self.key_hash,
                'h_limit': self.hash_limit,
                'header_len': self.header_len,
                'fmt': self.fmt,
                'bucket_size': self.bucket_size,
                'fmt_map': self.fmt_map,
                'dir': os.path.dirname(fn_),
                'num': int(fn_[fn_.rindex('_') + 1:]),
                }
        header_entry = '{0}{1}'.format(msgpack.dumps(header), HEADER_DELIM)
        fp_ = io.open(fn_, 'w+b')
        fp_.write(header_entry)
        header['fp'] = fp_
        self.tables[fn_] = header
        return header

    def data_entry(self, c_key, id_, start, size, type_, prev, **kwargs):
        '''
        Return the index data entry string
        '''
        entry = {
                'ckey': c_key,
                'st': start,
                'sz': size,
                'rev': sorbic.utils.gen_rev(),
                't': type_,
                'p': prev,
                }
        entry.update(kwargs)
        if not id_:
            entry['id'] = sorbic.utils.rand_hex_str(self.key_size)
        else:
            entry['id'] = id_
        packed = msgpack.dumps(entry)
        p_len = struct.pack('>H', len(packed))
        return '{0}{1}'.format(p_len, packed)

    def _table_map(self, comps, fmt_map):
        '''
        Convert a table map to a dict given the table's 
        '''
        ret = {}
        for ind in range(len(fmt_map)):
            ret[fmt_map[ind]] = comps[ind]
        return ret

    def get_table_entry(self, key, c_key):
        '''
        Return the entry location for the given key and crypt key pair
        '''
        root = self.entry_root(key)
        num = 0
        while True:
            table_fn = os.path.join(root, 'sorbic_table_{0}'.format(num))
            table = self.get_hash_table(table_fn)
            pos = _calc_pos(
                    c_key,
                    table['hash_limit'],
                    table['bucket_size'],
                    table['header_len'])
            table['fp'].seek(pos)
            bucket = table['fp'].read(table['bucket_size'])
            try:
                comps = struct.unpack(table['fmt'], bucket)
                if comps[0] == '\0' * self.key_size:
                    comps = (None, None)
            except Exception:
                comps = (None, None)
            ret = self._table_map(comps, table['fmt_map'])
            ret['pos'] = pos
            ret['tfn'] = table.name
            if ret['key'] is None:
                return ret
            if ret['key'] == c_key:
                return ret
            num += 1

    def write_table_entry(self, table_entry, c_key, prev):
        '''
        Write a table entry
        '''
        table = self.get_hash_table[table_entry['tfn']]
        t_str = struct.pack(table.fmt, c_key, prev)
        table.seek(table_entry['pos'])
        table.write(t_str)

    def write_data_entry(
            self,
            table_entry,
            c_key,
            id_,
            start,
            size,
            type_,
            **kwargs):
        '''
        Write a data entry
        '''
        table = self.get_hash_table[table_entry['tfn']]
        raw = self.data_entry(
                self,
                c_key,
                id_,
                start,
                size,
                type_,
                table_entry['prev'],
                **kwargs)
        table['fp'].seek(0, 2)
        prev = table['fp'].tell()
        table['fp'].write(raw)
        return prev

    def commit(
            self,
            table_entry,
            key,
            c_key,
            id_,
            start,
            size,
            type_,
            **kwargs):
        prev = self.write_data_entry(
                table_entry,
                c_key,
                id_,
                start,
                size,
                type_,
                **kwargs)
        self.write_table_entry(table_entry, c_key, prev)
