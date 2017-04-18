#!/usr/bin/env python3

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(name='qa-dataprovider',
      version='0.2.2',
      description='Wrapper for pandas DataReader and other data sources',
      author='',
      author_email='',
      # install_requires=[
      #     'pandas',
      #     'numpy',
      #     'requests_cache',
      #     'pandas-datareader',
      #     'pytz',
      #     'bizdays'
      # ],
      packages=['qa_dataprovider'],
      )
