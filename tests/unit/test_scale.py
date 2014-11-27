# -*- coding: utf-8 -*-
# Import sorbic libs
import sorbic.db

# Import python libs
import os
import shutil
import unittest
import tempfile


class TestScale(unittest.TestCase):
    '''
    '''
    def _many(self, entries=100000, **kwargs):
        '''
        run a scale db execution with the given db kwargs
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db = sorbic.db.DB(root, **kwargs)
        data = {1:1}
        for num in xrange(entries):
            key = str(num)
            db.insert(key, data)
        for num in xrange(entries):
            key = str(num)
            pull_data = db.get(key)
            self.assertEqual(data, pull_data)
        shutil.rmtree(w_dir)

    def test_many_top(self):
        self._many()

    def test_many_small_table(self):
        self._many(hash_limit=0xffff)

    def test_many_tiny_table(self):
        self._many(hash_limit=0xfff)

    def _test_too_many(self):
        '''
        This is disabled in normal tests, but it is meant to test the upper
        limits of key entries scale
        '''
        self._many(entries=10000000)
