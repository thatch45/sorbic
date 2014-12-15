# -*- coding: utf-8 -*-
# Import sorbic libs
import sorbic.db

# Import python libs
import os
import shutil
import unittest
import tempfile

try:
    import libnacl.blake  # pylint: disable=W0611
    HAS_BLAKE = True
except ImportError:
    HAS_BLAKE = False


class TestDB(unittest.TestCase):
    '''
    '''
    def _run_test(self, key_hash):
        w_dir = tempfile.mkdtemp()
        root = os.path.join(w_dir, 'db_root')
        db = sorbic.db.DB(root, key_hash=key_hash)
        data = {1:2}
        db.insert('foo', data)
        pull_data = db.get('foo')
        self.assertEqual(data, pull_data)
        shutil.rmtree(w_dir)

    def test_blake(self):
        if not HAS_BLAKE:
            return
        self._run_test('blake')

    def test_algos(self):
        # don't use hashlib.algorithms, need to support 2.6
        for algo in ('md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512'):
            self._run_test(algo)
