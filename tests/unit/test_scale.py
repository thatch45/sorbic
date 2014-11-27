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
    def _many(self, **kwargs):
        '''
        run a scale db execution with the given db kwargs
        '''
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db = sorbic.db.DB(root, **kwargs)
        data = {1:1}
        for num in xrange(100000):
            key = str(num)
            db.insert(key, data)
        for num in xrange(100000):
            key = str(num)
            pull_data = db.get(key)
            self.assertEqual(data, pull_data)
        shutil.rmtree(w_dir)

    def test_many_top(self):
        self._many()

    def test_many_small_table(self):
        self._many(hash_limit=0xffff)
