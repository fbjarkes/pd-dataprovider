#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import logging
import os

from qa_dataprovider.generic_dataprovider import GenericDataProvider
import pandas as pd



class CsvFileDataProvider(GenericDataProvider):
    """
    #https://www.quandl.com/search?query=

    #Download:
    #for i in `cat ~/Dropbox/tickers/ndx.txt`; do wget https://www.quandl.com/api/v3/datasets/WIKI/$i.csv ; done
    """

    logging.basicConfig(level=logging.DEBUG, format='%(filename)s: %(message)s')
    logger = logging

    def __init__(self, paths):
        """
        Initialize with a list of paths for which each call to get_data() tries to open
        csv file directly in paths. 
        
        For example calling get_data() with ticker 'SPY' using paths ['data/stocks', 'data/etf'] 
        will try to open 'data/stocks/SPY.csv' and 'data/etf/SPY.csv' with first found file 
        being used.
        
        :param list paths: A list of paths containing csv-files  
        """
        self.paths = paths

    def _get_data_internal(self, ticker, from_date, to_date, timeframe):

        for path in self.paths:
            file_path = "{}/{}.{}".format(path,ticker,'csv')
            if os.path.exists(file_path):
                with open(file_path) as f:
                    df = pd.read_csv(f)
                    df = df.set_index(pd.DatetimeIndex(df['Date'])).sort_index()
                    self.logger.info("{}, {:d} rows ({} to {})"
                                      .format(f.name, len(df), df.index[0], df.index[-1]))
                    return self._post_process(df, ticker, from_date, to_date, timeframe)

        self.logger.info("{} not found in {}".format(ticker, self.paths))


    def add_quotes(self, data, ticker):
        """
        Quotes are not available...         
        """
        return data


if __name__ == '__main__':
    paths = ["/home/fbjarkes/Dropbox/quandl/spy", "/home/fbjarkes/Dropbox/quandl/ndx",
             "/home/fbjarkes/Dropbox/quandl/iwm"]
    provider = CsvFileDataProvider(paths)
    dailys = provider.get_data(['DIS','KO','BA','MSFT'], '2010-01-01', '2016-12-31',
                               max_workers=5, timeframe='week')
    print(dailys[0].tail())
    print(dailys[1].tail())