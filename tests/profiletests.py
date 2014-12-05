#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Import python libs
import os
import sys
import unittest
import cProfile
import pstats
import StringIO

SORBIC_ROOT = os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
UNIT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), 'unit'))

sys.path.insert(0, SORBIC_ROOT)


def run_suite(path=UNIT_ROOT):
    loader = unittest.TestLoader()
    tests = loader.discover(path)
    pr_ = cProfile.Profile()
    pr_.enable()
    unittest.TextTestRunner(verbosity=2).run(tests)
    pr_.disable()
    string = StringIO.StringIO()
    sortby = 'cumulative'
    ps_ = pstats.Stats(pr_, stream=string).sort_stats(sortby)
    ps_.print_stats()
    print string.getvalue()


if __name__ == '__main__':
    run_suite()
