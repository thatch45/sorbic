#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Import python libs
import os
import sys
# Import sorbic libs
import sorbic

if 'USE_SETUPTOOLS' in os.environ or 'setuptools' in sys.modules:
    from setuptools import setup
else:
    from distutils.core import setup

NAME = 'sorbic'
DESC = ('Pure python distributed database (engine first)')
VERSION = sorbic.version

setup(name=NAME,
      version=VERSION,
      description=DESC,
      author='Thomas S Hatch',
      author_email='thatch@saltstack.com',
      url='https://github.com/thatch45/sorbic',
      classifiers=[
          'Operating System :: OS Independent',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3.4',
          'Development Status :: 2 - Pre-Alpha',
          'Topic :: Database',
          'Topic :: Database :: Database Engines/Servers',
          'Intended Audience :: Developers',
          ],
      packages=[
          'sorbic',
          'sorbic.ind',
          'sorbic.stor',
          'sorbic.utils',
          ]
      )
