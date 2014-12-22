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
        data = r'file contents!!!'
        db_.insert('foo', data, type_='file')
        pull_data = db_.get('foo')
        self.assertEqual(data, pull_data)
        shutil.rmtree(w_dir)
