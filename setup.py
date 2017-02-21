#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='dataprovider',
      version='0.1',
      description='Wrapper for pandas DataReader',
      author='',
      author_email='',
      packages=['dataprovider'],
     )