# -*- coding: utf-8 -*-
'''
Test core database functions
'''
# Import sorbic libs
import sorbic.db
import sorbic.utils.rand

# Import python libs
import os
import shutil
import unittest
import tempfile


class TestDB(unittest.TestCase):
    '''
    Cover db funcs
    '''
    def test_create(self):
        '''
        Test database creation
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db_ = sorbic.db.DB(root)
        data = {1:2}
        db_.insert('foo', data)
        pull_data = db_.get('foo')
        self.assertEqual(data, pull_data)
        shutil.rmtree(w_dir)

    def test_listdir(self):
        '''
        Test basic listdir
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db_ = sorbic.db.DB(root, hash_limit=0xfffff)
        data = {1:2}
        db_.insert('foo/bar/baz', data)
        pull_data = db_.listdir('foo/bar')
        self.assertEqual(pull_data[0]['key'], 'foo/bar/baz')
        shutil.rmtree(w_dir)

    def test_listdir_many(self):
        '''
        Test many entries in listdir
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db_ = sorbic.db.DB(root, hash_limit=0xfffff)
        data = {1:2}
        keys = set([
            'foo/bar/1',
            'foo/bar/2',
            'foo/bar/3',
            'foo/bar/4',
            'foo/bar/5',
            'foo/bar/6'])
        for key in keys:
            db_.insert(key, data)
        pull_data = db_.listdir('foo/bar')
        pull_set = set()
        for chunk in pull_data:
            pull_set.add(chunk['key'])
        self.assertEqual(pull_set, keys)
        shutil.rmtree(w_dir)

    def test_json(self):
        '''
        Test using json for index data
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db_ = sorbic.db.DB(root, serial='json')
        data = {u'1':2}
        db_.insert('foo', data)
        pull_data = db_.get('foo')
        self.assertEqual(data, pull_data)
        shutil.rmtree(w_dir)

    def test_higherarchy(self):
        '''
        Test that the higherarchy of directories works
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db_ = sorbic.db.DB(root)
        data = {1:2}
        key = 'foo/bar/baz'
        c_key = db_.index.raw_crypt_key(key)
        db_.insert(key, data)
        pull_data = db_.get(key)
        self.assertEqual(data, pull_data)
        table_entry = db_.index.get_table_entry(key, c_key)
        self.assertEqual(
                table_entry['tfn'],
                os.path.join(root, 'foo', 'bar', 'sorbic_table_0'))
        shutil.rmtree(w_dir)

    def test_r_key(self):
        '''
        Verify that the relative key is stored and retrived
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db_ = sorbic.db.DB(root)
        data = {1:2}
        full_key = 'foo/bar/baz/key'
        db_.insert(full_key, data)
        pull_data = db_.get(full_key, meta=True)
        self.assertEqual(data, pull_data['data'])
        self.assertEqual(full_key, pull_data['meta']['data']['key'])
        shutil.rmtree(w_dir)

    def test_top_loc(self):
        '''
        Verify that keys in / work
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db_ = sorbic.db.DB(root)
        data = {1:2}
        key = 'orange'
        c_key = db_.index.raw_crypt_key(key)
        db_.insert(key, data)
        pull_data = db_.get(key)
        self.assertEqual(data, pull_data)
        table_entry = db_.index.get_table_entry(key, c_key)
        self.assertEqual(
                table_entry['tfn'],
                os.path.join(root, 'sorbic_table_0'))
        shutil.rmtree(w_dir)

    def test_inv_fmt(self):
        '''
        verify that the table bucket format can be changed
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db_ = sorbic.db.DB(root, fmt='<KsQH', fmt_map=['key', 'prev', 'rev'])
        data = {1:2}
        db_.insert('foo', data)
        pull_data = db_.get('foo')
        self.assertEqual(data, pull_data)
        shutil.rmtree(w_dir)

    def test_id(self):
        '''
        Verify id writing and retrival
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db_ = sorbic.db.DB(root)
        data = {1:2}
        key = 'somekey'
        entries = []
        for num in range(10):
            data = {1: num}
            entry = db_.insert(key, data)
            entries.append({'entry': entry, 'data': data})
        for entry in entries:
            self.assertEqual(db_.get(key, entry['entry']['id']), entry['data'])
        shutil.rmtree(w_dir)

    def test_rmdir(self):
        '''
        Verify that the directory is removed and tht other data is untouched
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db_ = sorbic.db.DB(root)
        data = {1:2}
        key = 'foo/bar/baz'
        key2 = 'foo/bar/baz_2'
        key3 = 'foo/quo/qux'
        db_.insert(key, data)
        db_.insert(key2, data)
        db_.insert(key3, data)
        db_.rmdir('foo/bar')
        pull_data = db_.get(key)
        pull_data2 = db_.get(key2)
        pull_data3 = db_.get(key3)
        self.assertIsNone(pull_data)
        self.assertIsNone(pull_data2)
        self.assertEqual(data, pull_data3)
        shutil.rmtree(w_dir)

    def test_rm(self):
        '''
        Verify that the key is removed by the rm routine
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db_ = sorbic.db.DB(root)
        data = {1:2}
        key = 'foo/bar/baz'
        db_.insert(key, data)
        db_.rm('foo/bar/baz')
        pull_data = db_.get(key)
        self.assertIsNone(pull_data)
        shutil.rmtree(w_dir)

    def test_rm_collide(self):
        # 13875 and 99991 are known to colide
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db_ = sorbic.db.DB(root)
        data1 = {1:1}
        data2 = {1:2}
        key1 = '13875'
        key2 = '99991'
        db_.insert(key1, data1)
        db_.insert(key2, data2)
        db_.rm(key1)
        pull_data = db_.get(key2)
        self.assertEqual(pull_data, data2)
        shutil.rmtree(w_dir)

    def test_doc_data(self):
        '''
        Test database creation
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db_ = sorbic.db.DB(root)
        data = {'cheese': {'spam': {'bacon': 'end'}}}
        db_.insert('foo', data)
        pull_data = db_.get('foo', doc_path='cheese:spam:bacon')
        self.assertEqual('end', pull_data)
        shutil.rmtree(w_dir)

    def test_extra_index_data(self):
        '''
        Test adding arbitrary data to the index dataset
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db_ = sorbic.db.DB(root)
        for _ in range(100):
            data = sorbic.utils.rand.rand_dict()
            while True:
                try:
                    ind_extra = sorbic.utils.rand.rand_dict()
                    top_key = next(iter(ind_extra))
                except StopIteration:
                    # Bad generation of data
                    continue
                break
            db_.insert('foo', data, **ind_extra)
            pull_data = db_.get('foo', meta=True)
            self.assertIn(top_key, pull_data['meta']['data'])
