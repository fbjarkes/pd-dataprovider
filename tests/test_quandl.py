#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import unittest
import logging


import pandas as pd

#TODO: imports installed lib?!
from qa_dataprovider.generic_dataprovider import GenericDataProvider



class QuandlDataProvider(GenericDataProvider):
    logging.basicConfig(level=logging.DEBUG, format='%(filename)s: %(message)s')
    logger = logging.getLogger(__file__)

    _name = "QuandlDataProvider"
    _paths = ["~/Dropbox/quandl/spy", "~/Dropbox/quandl/ndx", "~/Dropbox/quandl/iwm"]

    def __init__(self, paths=_paths):
        super(QuandlDataProvider, self).__init__(quotes=False, trading_days=True, meta_data=True)
        self.paths = paths

    def get_data(self, tickers, param1, param2):
        df_list = []
        for ticker in tickers:
            for path in self.paths:
                with open(path + "/" + ticker + ".csv") as f:
                    df = pd.read_csv(f)
                    df = df.set_index(['Date']).sort_index()
                    self.logger.debug("{}, {:d} rows ({:s} to {:s})"
                                      .format(f.name, len(df), df.index[0], df.index[-1]))
                    df_list.append(df)

        return df_list

    def get_quote(self, ticker, provider):
        pass

    def get_data_parallel(self, tickers, from_date, to_date, max_workers=5, timeframe='day'):
        pass

class TestData(unittest.TestCase):

    provider = QuandlDataProvider(["data"])

    def test_daily_trading_days(self):

        daily = self.provider.get_data(['DIS'],'2010-01-01','2017-01-01')[0]


        assert daily.loc['2016-01-04']['Day'] == 1 # Monday Jan. 4
        #assert daily.loc['20160108']['Day'] == 5 # Friday Jan. 8
        #assert daily.loc['20160111']['Day'] == 6  # Monday Jan. 11
        #assert daily.loc['20161230']['Day'] == 252  #240? Friday Dec. 30


    #def test_weekly_volume(self):
    #    daily = self.provider.get_data('SPY','2010-01-01','2017-01-01')
    #    weekly = self.provider.get_data('SPY', '2010-01-01', '2017-01-01', timeframe='week')
    #    #TODO: assert volume in weekly bar is the sum of its days

if __name__ == '__main__':
    
    unittest.main()