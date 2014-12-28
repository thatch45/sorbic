# -*- coding: utf-8 -*-
'''
Test database compression functions
'''
# Import sorbic libs
import sorbic.db

# Import python libs
import os
import shutil
import unittest
import tempfile


class TestCompress(unittest.TestCase):
    '''
    Cover compression possibilities
    '''
    def test_compress_no_changes(self):
        '''
        Run a scale db execution with the given db kwargs
        '''
        entries = 100
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db = sorbic.db.DB(root)
        data = {1:1}
        for num in xrange(entries):
            key = str(num)
            db.insert(key, data)
        db.compress('', 0)
        for num in xrange(entries):
            key = str(num)
            pull_data = db.get(key)
            self.assertEqual(data, pull_data)
        shutil.rmtree(w_dir)

    def _test_compress_no_changes_depth(self):
        '''
        Run a scale db execution with the given db kwargs
        '''
        entries = 100
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db = sorbic.db.DB(root)
        data = {1:1}
        key = 'foo/bar'
        for num in xrange(entries):
            db.insert(key, data)
        db.compress('foo', 0)
        for num in xrange(entries):
            pull_data = db.get(key)
            self.assertEqual(data, pull_data)
        shutil.rmtree(w_dir)
