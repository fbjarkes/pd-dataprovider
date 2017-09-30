#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import unittest

from qa_dataprovider.csv_dataprovider import CsvFileDataProvider
from qa_dataprovider.web_dataprovider import CachedWebDataProvider


class TestPostProcessor(unittest.TestCase):


    def test_filter_rth(self):
        provider = CsvFileDataProvider(["data"])

        df = provider.get_data(['aapl_5min'], '2017-07-01', '2017-08-01', timeframe='5min',
                               transform='5min', rth=True)[0]
        assert len(df.at_time('16:05:00')) == 0

    def test_validate_timeframe(self):
        provider = CsvFileDataProvider(["data"])
        df_list = provider.get_data(['aapl_5min'], '2017-07-01', '2017-08-01', timeframe='day', rth=True)
        assert len(df_list) == 0

if __name__ == '__main__':
    unittest.main()