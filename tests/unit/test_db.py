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
