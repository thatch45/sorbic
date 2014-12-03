#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Import python libs
import os
import sys

# Import Salt testing libs
from salttesting.parser.cover import SaltCoverageTestingParser

SORBIC_ROOT = os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
UNIT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), 'unit'))

sys.path.insert(0, SORBIC_ROOT)

class SorbicTestingParser(SaltCoverageTestingParser):
    source_code_basedir = SORBIC_ROOT

    def __init__(self, *args, **kwargs):
        SaltCoverageTestingParser.__init__(self, UNIT_ROOT, *args, **kwargs)
        self.option_groups.remove(self.fs_cleanup_options_group)


if __name__ == '__main__':
    try:
        parser = SorbicTestingParser()
        parser.parse_args()
        parser.start_coverage(
            branch=True,
            source=[os.path.join(SORBIC_ROOT, 'sorbic')],
        )
        overall_status = []
        if parser.options.name:
            for name in parser.options.name:
                overall_status.append(
                    parser.run_suite('', name, load_from_name=True)
                )
        else:
            overall_status.append(
                parser.run_suite(UNIT_ROOT, 'Unit Tests')
            )
        if overall_status.count(False) > 0:
            parser.finalize(1)
        parser.finalize(0)
    except KeyboardInterrupt:
        print('\nCaught keyboard interrupt. Exiting.\n')
        exit(0)
