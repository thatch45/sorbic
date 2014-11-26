# -*- coding: utf-8 -*-
# Import sorbic libs
import sorbic.db

# Import python libs
import os
import shutil
import unittest
import tempfile


class TestDB(unittest.TestCase):
    '''
    '''
    def test_create(self):
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db = sorbic.db.DB(root)
        data = {1:2}
        db.insert('foo', data)
        pull_data = db.get('foo')
        self.assertEqual(data, pull_data)
        shutil.rmtree(w_dir)

    def test_higherarchy(self):
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db = sorbic.db.DB(root)
        data = {1:2}
        key = 'foo/bar/baz'
        c_key = db.index.raw_crypt_key(key)
        db.insert(key, data)
        pull_data = db.get(key)
        self.assertEqual(data, pull_data)
        table_entry = db.index.get_table_entry(key, c_key)
        self.assertEqual(
                table_entry['tfn'],
                os.path.join(root, 'foo', 'bar', 'sorbic_table_0'))
        shutil.rmtree(w_dir)

    def test_top_loc(self):
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db = sorbic.db.DB(root)
        data = {1:2}
        key = 'orange'
        c_key = db.index.raw_crypt_key(key)
        db.insert(key, data)
        pull_data = db.get(key)
        self.assertEqual(data, pull_data)
        table_entry = db.index.get_table_entry(key, c_key)
        self.assertEqual(
                table_entry['tfn'],
                os.path.join(root, 'sorbic_table_0'))
        shutil.rmtree(w_dir)

    def test_id(self):
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db = sorbic.db.DB(root)
        data = {1:2}
        key = 'somekey'
        entries = []
        for num in range(10):
            data = {1: num}
            entry = db.insert(key, data)
            entries.append({'entry': entry, 'data': data})
        for entry in entries:
            self.assertEqual(db.get(key, entry['entry']['id']), entry['data'])
