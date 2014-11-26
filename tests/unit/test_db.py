# -*- coding: utf-8 -*-
# Import sorbic libs
import sorbic.db

# Import python libs
import os
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
