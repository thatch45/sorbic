'''
Interface to interact on a database level
'''
# Import python libs
import os
import io
import shutil
# Import sorbic libs
import sorbic.ind.hdht
import sorbic.stor.files
import sorbic.utils.traverse
# Import third party libs
import msgpack

DB_OPTS = (
        'key_delim',
        'hash_limit',
        'key_hash',
        'fmt',
        'fmt_map',
        'header_len',
        'serial')


class DB(object):
    '''
    Databaseing
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
        self.root = root
        self.key_delim = key_delim
        self.hash_limit = hash_limit
        self.key_hash = key_hash
        self.fmt = fmt
        self.fmt_map = fmt_map
        self.header_len = header_len
        self.serial = serial
        self._get_db_meta()
        self.index = sorbic.ind.hdht.HDHT(
            self.root,
            self.key_delim,
            self.hash_limit,
            self.key_hash,
            self.fmt,
            self.fmt_map,
            self.header_len)
        self.write_stor_funcs = self.__gen_write_stor_funcs()
        self.read_stor_funcs = self.__gen_read_stor_funcs()

    def __gen_write_stor_funcs(self):
        '''
        Return the storage write functions dict mapping to types
        '''
        return {'doc': self.index.write_doc_stor,
                'file': sorbic.stor.files.write}

    def __gen_read_stor_funcs(self):
        '''
        Return the storage read functions dict mapping to types
        '''
        return {'doc': self.index.read_doc_stor,
                'file': sorbic.stor.files.read}

    def _get_db_meta(self):
        '''
        Read in the database metadata to preserve the original behavior
        as to when the database are created
        '''
        db_meta = os.path.join(self.root, 'sorbic_db_meta.mp')
        meta = {}
        if os.path.isfile(db_meta):
            with io.open(db_meta, 'rb') as fp_:
                meta = msgpack.loads(fp_.read())
        for entry in DB_OPTS:
            meta[entry] = meta.get(entry, getattr(self, entry))
            setattr(self, entry, meta[entry])
        if not os.path.isdir(self.root):
            os.makedirs(self.root)
        with io.open(db_meta, 'w+b') as fp_:
            fp_.write(msgpack.dumps(meta))

    def _get_storage(self, entries, **kwargs):
        stor = self.read_stor_funcs[entries['data']['t']](entries, self.serial, **kwargs)
        return stor

    def write_stor(self, table_entry, data, serial, type_):
        '''
        Write the applicable storage type subsytem
        '''
        return self.write_stor_funcs[type_](
                 table_entry,
                 data,
                 serial)

    def insert(self, key, data, id_=None, type_='doc', serial=None, **kwargs):
        '''
        Insert a key into the database
        '''
        c_key = self.index.raw_crypt_key(key)
        table_entry = self.index.get_table_entry(key, c_key)
        serial = serial if serial else self.serial
        kwargs.update(self.write_stor(
            table_entry,
            data,
            serial,
            type_))
        return self.index.commit(
            table_entry,
            key,
            c_key,
            id_,
            type_,
            **kwargs)

    def get_meta(self, key, id_=None, count=None):
        '''
        Retrive a meta entry
        '''
        return self.index.get_index_entry(key, id_, count)

    def get(self, key, id_=None, meta=False, count=None, **kwargs):
        '''
        Retrive a data entry
        '''
        entries = self.get_meta(key, id_, count)
        if not entries:
            return None
        if count:
            ret = []
            for index_entry in entries['data']:
                meta_entries = {'table': entries['table'], 'data': index_entry}
                stor_ret = self._get_storage(meta_entries, **kwargs)
                if meta:
                    ret.append({'data': stor_ret, 'meta': index_entry})
                else:
                    ret.append(self._get_storage(meta_entries, **kwargs))
            return ret
        if not meta:
            return self._get_storage(entries, **kwargs)
        else:
            ret = {}
            ret['data'] = self._get_storage(entries, **kwargs)
            ret['meta'] = entries
            return ret

    def compress(self, d_key=None, num=None):
        '''
        Compress a single given index, remove any associated data
        '''
        fn_root = self.root
        if not d_key or d_key == self.key_delim:
            pass
        else:
            fn_root = self.index.entry_root('{0}/blank'.format(d_key))
        fn_ = os.path.join(fn_root, 'sorbic_table_{0}'.format(num))
        trans_fn = os.path.join(fn_, '_trans')
        if os.path.exists(trans_fn):
            os.remove(trans_fn)
        trans_table = self.index.get_hash_table(trans_fn)
        table = self.index.get_hash_table(fn_)
        for entry in self.index._get_table_entries(fn_):
            self._compress_entry(entry, table, trans_table)
        shutil.move(trans_fn, fn_)

    def _compress_entry(self, entry, table, trans_table):
        '''
        Read the table entries to keep out of the given entry and write them
        fresh to the trans table
        '''
        c_key = self.index.raw_crypt_key(entry['key'])
        i_entries = self.index.get_index_entry(entry['key'], count=0xffffffff)
        keeps = []
        for ind in reversed(range(len(i_entries))):
            i_entry = i_entries[ind]
            if i_entry['_status'] != 'k':
                continue
            keeps.append(i_entry)
        for i_entry in keeps:
            serial = i_entry.get('serial', self.serial)
            stor = self._get_storage(i_entry)
            i_entry.update(self.write_stor(
                trans_table,
                stor,
                serial,
                i_entry['type']))
            kwargs = i_entry
            key = kwargs.pop('key')
            id_ = kwargs.pop('id')
            type_ = kwargs.pop('type')
            self.index.commit(trans_table, key, c_key, id_, type_, **kwargs)

    def listdir(self, d_key):
        '''
        List the contents of a directory
        '''
        return self.index.listdir(d_key)

    def rmdir(self, d_key):
        '''
        Recursively remove a key directory and all subdirs and subkeys.
        THIS OPERATION IS IRREVERSIBLE!!
        '''
        return self.index.rmdir(d_key)

    def rm(self, key, id_=None):
        '''
        Make a key for deletion, if the id is omitted then the key itself
        and all revs will be removed. THIS OPERATION IS IRREVERSIBLE!!
        '''
        return self.index.rm_key(key, id_)
