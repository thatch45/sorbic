# -*- coding: utf-8 -*-
# Import sorbic libs
import sorbic.db

# Import python libs
import os
import unittest
import tempfile


class TestScale(unittest.TestCase):
    '''
    '''
    def test_many_top(self):
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db = sorbic.db.DB(root)
        data = {1:1}
        for num in xrange(100000):
            key = str(num)
            db.insert(key, data)
        for num in xrange(100000):
            key = str(num)
            pull_data = db.get(key)
            self.assertEqual(data, pull_data)
