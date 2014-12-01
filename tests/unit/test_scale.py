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

    def _deep(self, entries=1000, **kwargs):
        '''
        Run a scale db test based on the depth of the id keys
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db = sorbic.db.DB(root, **kwargs)
        key = 'foo'
        ids = []
        for num in xrange(entries):
            e_data = db.insert(key, {1: num})
            ids.append(e_data)
        for num in xrange(entries):
            pull_data = db.get(key, ids[num]['id'], True)
            self.assertEqual({1: num}, pull_data['data'])
            self.assertEqual(ids[num]['rev'], pull_data['meta']['table']['rev'])
        every = db.get(key, count=entries)
        for num in xrange(entries):
            self.assertEqual({1: entries - (num + 1)}, every[num]['data'])
        shutil.rmtree(w_dir)

    def test_many_keys_top(self):
        self._many()

    def test_many_keys_small_table(self):
        self._many(hash_limit=0xffff)

    def test_many_keys_tiny_table(self):
        self._many(hash_limit=0xfff)

    def test_many_ids_top(self):
        self._deep()

    def _test_too_many_keys(self):
        '''
        This is disabled in normal tests, but it is meant to test the upper
        limits of key entries scale
        '''
        self._many(entries=10000000)

    def _test_too_many_ids(self):
        '''
        This is disabled in normal tests, but it is meant to test the upper
        limits of key entries scale
        '''
        self._deep(entries=10000000)
