#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import unittest

from qa_dataprovider.csv_dataprovider import CsvFileDataProvider
from qa_dataprovider.web_dataprovider import CachedWebDataProvider

class TestTimeFrames(unittest.TestCase):

    provider = CachedWebDataProvider('google')
    file_provider = CsvFileDataProvider(['data'])

    def test_daily_to_weekly(self):
        spy_daily = self.provider.get_data(['SPY'],'2016-01-01','2017-01-01')[0]
        spy_weekly = self.provider.get_data(['SPY'], '2016-01-01', '2017-01-01',
                                            timeframe='week')[0]

        assert spy_daily.loc['20160502']['Open'] == spy_weekly.loc['20160502']['Open']
        assert spy_daily.loc['20160506']['Close'] == spy_weekly.loc['20160502']['Close']

    def test_daily_to_monthly(self):
        dis_daily = self.file_provider.get_data(['DIS'], '2000-01-01', '2017-01-01')[0]
        dis_monthly = self.file_provider.get_data(['DIS'], '2000-01-01', '2017-01-01',
                                                timeframe='month')[0]
        assert dis_daily.loc['20160502']['Open'] == dis_monthly.loc['20160502']['Open']
        assert dis_daily.loc['20160531']['Close'] == dis_monthly.loc['20160502']['Close']
        assert dis_daily.loc['20160519']['Low'] == dis_monthly.loc['20160502']['Low']

if __name__ == '__main__':
    unittest.main()