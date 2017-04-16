#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import unittest

from dataprovider.dataprovider import CachedDataProvider

class TestTimeFrames(unittest.TestCase):

    provider = CachedDataProvider(cache_name='test_timeframes',expire_days=0)

    def test_daily_to_weekly(self):
        spy_daily = self.provider.get_data('SPY','2016-01-01','2017-01-01')
        spy_weekly = self.provider.get_data('SPY', '2016-01-01', '2017-01-01', timeframe='week')

        assert spy_daily.loc['20160502']['Open'] == spy_weekly.loc['20160502']['Open']
        assert spy_daily.loc['20160506']['Close'] == spy_weekly.loc['20160502']['Close']


    def daily_to_monthly(self):
        #TODO: verify monthly conversion
        pass


if __name__ == '__main__':
    unittest.main()