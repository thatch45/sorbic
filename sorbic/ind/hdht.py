# -*- coding: utf-8 -*-
'''
Higherarchical Distributed Hash Table index
'''
# Import python libs
import os
import io
import shutil
import struct
import hashlib

# Import sorbic libs
import sorbic.utils.rand
import sorbic.utils.traverse
import sorbic.stor.serial

# Import Third Party Libs
import msgpack

# index header types:
# k: "keep" the entry an the data
# r: "remove" the entry and the data
# e: "expired" remove the index entry but keep the data, another entry
#    references it
HEADER_DELIM = '_||_||_'
IND_HEAD_FMT = '>Hc'


def _calc_pos(c_key, hash_limit, b_size, header_len):
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
            hash_limit=0xfffff,
            key_hash='sha1',
            fmt='>KsQH',
            fmt_map=None,
            header_len=1024,
            serial='msgpack'):
        if fmt_map is None:
            self.fmt_map = ('key', 'prev', 'rev')
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
        self.serial = sorbic.stor.serial.Serial(serial)
        self.tables = {}

    def __crypt_func(self):
        '''
        Return the function to use to crypt hash index keys
        '''
        if self.key_hash.startswith('blake'):
            import libnacl.blake
            return libnacl.blake.blake2b
        return getattr(hashlib, self.key_hash)

    def __gen_key_size(self):
        '''
        Return the length of the crypt_key
        '''
        return len(self.raw_crypt_key('sorbic is awesome'))

    def __gen_bucket_size(self):
        '''
        Calculate the size of the index buckets
        '''
        args = []
        for arg in self.fmt_map:
            if arg == 'key':
                args.append('0' * self.key_size)
            elif arg == 'prev':
                args.append(1)
            elif arg == 'rev':
                args.append(1)
        return len(struct.pack(self.fmt, *args))

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
        if self.key_delim not in key:
            return self.root
        root = key[:key.rfind(self.key_delim)].replace(self.key_delim, os.sep)
        return os.path.join(self.root, root)

    def entry_base(self, key):
        '''
        Return the key basename
        '''
        if self.key_delim not in key:
            return key
        key = key.strip(self.key_delim)
        return key[key.rfind(self.key_delim):].replace(self.key_delim, os.sep).lstrip(self.key_delim)

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
            'hash_limit': self.hash_limit,
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
        fp_.seek(((self.hash_limit + 2) * self.bucket_size) + self.header_len)
        fp_.write('\0')
        header['fp'] = fp_
        self.tables[fn_] = header
        return header

    def index_entry(self, key, id_, type_, prev, **kwargs):
        '''
        Return the index data entry string
        '''
        entry = {
            'key': key,
            't': type_,
            'p': prev,
            }
        entry.update(kwargs)
        if not id_:
            entry['id'] = sorbic.utils.rand.gen_id()
        else:
            entry['id'] = id_
        packed = msgpack.dumps(entry)
        p_len = struct.pack(IND_HEAD_FMT, len(packed), 'k')
        return '{0}{1}'.format(p_len, packed), entry

    def _table_map(self, comps, fmt_map):
        '''
        Convert a table map to a dict
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
                    comps = (None, None, -1)
            except struct.error:
                comps = (None, None, -1)
            ret = self._table_map(comps, table['fmt_map'])
            ret['pos'] = pos
            ret['tfn'] = table['fp'].name
            ret['num'] = num
            if ret['key'] is None:
                return ret
            if ret['key'] == c_key:
                return ret
            # Adding these lines in will show keys that collide
            # in the hash table in the tests
            #print('***************')
            #print(self._read_index_entry(table, ret['prev']))
            #print(key)
            #print('***************')
            num += 1

    def _read_index_entry(self, table, prev):
        table['fp'].seek(prev)
        table['fp'].seek(prev)
        data_head = struct.unpack(IND_HEAD_FMT, table['fp'].read(3))
        return msgpack.loads(table['fp'].read(data_head[0]))

    def get_index_entry(self, key, id_=None, count=None):
        '''
        Get the data entry for the given key
        '''
        ret = {}
        c_key = self.raw_crypt_key(key)
        table_entry = self.get_table_entry(key, c_key)
        if not table_entry['key']:
            return None
        table = self.tables[table_entry['tfn']]
        prev = table_entry['prev']
        if prev == 0:
            # There is no data, stubbed out for deletion, return None
            return None
        ret['table'] = table_entry
        rev = table_entry['rev']
        counted = 0
        rets = {'data': [], 'table': table_entry}
        while True:
            index_entry = self._read_index_entry(table, prev)
            ret['data'] = index_entry
            if id_:
                if index_entry['id'] == id_:
                    ret['table']['rev'] = rev
                    return ret
                if index_entry['p']:
                    prev = index_entry['p']
                    rev -= 1
                    continue
                return ret
            elif count:
                if counted < count:
                    rets['data'].append(index_entry)
                    counted += 1
                    prev = index_entry['p']
                    if prev is None:
                        return rets
                else:
                    return rets
            else:
                return ret

    def _get_table_entries(self, fn_):
        '''
        Return the table entries in a given table
        '''
        table = self.get_hash_table(fn_)
        table['fp'].seek(table['header_len'])
        seek_lim = ((self.hash_limit + 2) * self.bucket_size) + self.header_len
        while True:
            bucket = table['fp'].read(table['bucket_size'])
            if table['fp'].tell() > seek_lim:
                break
            if bucket.startswith('\0'):
                continue
            try:
                comps = struct.unpack(table['fmt'], bucket)
                if comps[0] == '\0' * self.key_size:
                    comps = (None, None, -1)
            except struct.error:
                comps = (None, None, -1)
            if not comps[0]:
                continue
            ret = self._table_map(comps, table['fmt_map'])
            data = self._read_index_entry(table, ret['prev'])
            ret['key'] = data['key']
            yield ret

    def rm_key(self, key, id_=None):
        '''
        Remove a key id_, if no id_ is specified the key is recursively removed
        '''
        ret = False
        c_key = self.raw_crypt_key(key)
        table_entry = self.get_table_entry(key, c_key)
        table = self.tables[table_entry['tfn']]
        prev = table_entry['prev']
        while True:
            stub = True
            table['fp'].seek(prev)
            data_head = struct.unpack(IND_HEAD_FMT, table['fp'].read(3))
            index_entry = msgpack.loads(table['fp'].read(data_head[0]))
            if id_:
                if index_entry['id'] != id_:
                    stub = False
            else:
                stub = True
            if stub:
                table['fp'].seek(prev)
                table['fp'].write(struct.pack(IND_HEAD_FMT, data_head[0], 'r'))
                ret = True
                if id_:
                    break
            prev = index_entry['p']
            if not prev:
                break
        if not id_:
            # Chck if the next table has a collision entry, if so keey this
            # table entry and mark it for removal in a compact call
            next_fn = '{0}{1}'.format(table['fp'].name[:-1], table['num'] + 1)
            collision = False
            if os.path.isfile(next_fn):
                next_table = self.get_hash_table(next_fn)
                next_table['fp'].seek(table_entry['pos'])
                next_raw_entry = next_table['fp'].read(next_table['bucket_size'])
                if next_raw_entry != '\0' * next_table['bucket_size']:
                    collision = True
            # Stub out the table entry as well
            if not collision:
                stub_entry = '\0' * table['bucket_size']
            else:
                stub_entry = struct.pack(table['fmt'], table_entry['key'], 0, 0)
            table['fp'].seek(table_entry['pos'])
            table['fp'].write(stub_entry)
            ret = True
        return ret

    def rmdir(self, d_key):
        '''
        Recursively remove a key directory and all keys and key data
        therein and below.
        '''
        fn_root = self.root
        if not d_key or d_key == self.key_delim:
            pass
        else:
            fn_root = self.entry_root('{0}/blank'.format(d_key))
        shutil.rmtree(fn_root)
        return True

    def listdir(self, d_key):
        '''
        Return a list of the keys
        '''
        fn_root = self.root
        ret = []
        if not d_key or d_key == self.key_delim:
            pass
        else:
            fn_root = self.entry_root('{0}/blank'.format(d_key))
        for fn_ in os.listdir(fn_root):
            if not fn_.startswith('sorbic_table_'):
                continue
            full = os.path.join(fn_root, fn_)
            for entry in self._get_table_entries(full):
                ret.append(entry)
        return ret

    def write_table_entry(self, table_entry, c_key, prev):
        '''
        Write a table entry
        '''
        table = self.get_hash_table(table_entry['tfn'])
        t_str = struct.pack(table['fmt'], c_key, prev, table_entry['rev'] + 1)
        table['fp'].seek(table_entry['pos'])
        table['fp'].write(t_str)
        return table_entry['rev'] + 1

    def write_index_entry(
            self,
            table_entry,
            key,
            id_,
            type_,
            **kwargs):
        '''
        Write a data entry
        '''
        table = self.get_hash_table(table_entry['tfn'])
        raw, entry = self.index_entry(
            key,
            id_,
            type_,
            table_entry['prev'],
            **kwargs)
        table['fp'].seek(0, 2)
        prev = table['fp'].tell()
        table['fp'].write(raw)
        return prev, entry

    def commit(
            self,
            table_entry,
            key,
            c_key,
            id_,
            type_,
            **kwargs):
        prev, entry = self.write_index_entry(
            table_entry,
            key,
            id_,
            type_,
            **kwargs)
        entry['rev'] = self.write_table_entry(table_entry, c_key, prev)
        return entry

    def compress(self, d_key, num):
        '''
        Compress a single given index, remove any associated data
        '''
        fn_root = self.root
        if not d_key or d_key == self.key_delim:
            pass
        else:
            fn_root = self.entry_root('{0}/blank'.format(d_key))
        fn_ = os.path.join(fn_root, 'sorbic_table_{0}'.format(num))
        table = self._get_hash_table(fn_)

    def write_doc_stor(self, table_entry, data, serial=None):
        '''
        Write the data to the storage file
        '''
        table = self.get_hash_table(table_entry['tfn'])
        serial = serial if serial else self.serial.default
        serial_fun = getattr(self.serial, '{0}_dump'.format(serial))
        serial_data = serial_fun(data)
        table['fp'].seek(0, 2)
        start = table['fp'].tell()
        table['fp'].write(serial_data)
        return {'st': start, 'sz': len(serial_data)}

    def read_doc_stor(self, entries, serial=None, **kwargs):
        '''
        Read in the data
        '''
        table = self.get_hash_table(entries['table']['tfn'])
        table['fp'].seek(entries['data']['st'])
        raw = table['fp'].read(entries['data']['sz'])
        serial = serial if serial else self.serial.default
        serial_fun = getattr(self.serial, '{0}_load'.format(serial))
        ret = serial_fun(raw)
        if kwargs.get('doc_path'):
            return sorbic.utils.traverse.traverse_dict_and_list(ret, kwargs['doc_path'])
        return ret
