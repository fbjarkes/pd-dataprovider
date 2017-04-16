#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import unittest

from dataprovider.dataprovider import CachedDataProvider

class TestData(unittest.TestCase):

    provider = CachedDataProvider(cache_name='test_data', expire_days=0, trading_days=True)

    def test_daily_trading_days(self):
        spy_daily = self.provider.get_data('SPY','2010-01-01','2017-01-01')
        assert spy_daily.loc['20160104']['Day'] == 1 # Monday Jan. 4
        assert spy_daily.loc['20160108']['Day'] == 5 # Friday Jan. 8
        assert spy_daily.loc['20160111']['Day'] == 6  # Monday Jan. 11
        assert spy_daily.loc['20161230']['Day'] == 252  #240? Friday Dec. 30


    def test_weekly_volume(self):
        spy_daily = self.provider.get_data('SPY','2010-01-01','2017-01-01')
        spy_weekly = self.provider.get_data('SPY', '2010-01-01', '2017-01-01', timeframe='week')
        #TODO: assert volume in weekly bar is the sum of its days

if __name__ == '__main__':
    unittest.main()