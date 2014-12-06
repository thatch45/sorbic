# -*- coding: utf-8 -*-
'''
Test core database functions
'''
# Import sorbic libs
import sorbic.db

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
        self.assertEqual(pull_data[0]['key'], 'baz')
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
        r_key = 'key'
        db_.insert(full_key, data)
        pull_data = db_.get(full_key, meta=True)
        self.assertEqual(data, pull_data['data'])
        self.assertEqual(r_key, pull_data['meta']['data']['r_key'])
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
