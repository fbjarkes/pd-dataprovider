#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-


import logging
import os

from qa_dataprovider.generic_dataprovider import GenericDataProvider
import pandas as pd



class CsvFileDataProvider(GenericDataProvider):
    """
    Expecting data to have Columns (case sensitive): Date, Open, High, Low, Close, Volume
    Example:
        Date,Open,High,Low,Close,Change,Settle,Volume,Previous Day Open Interest
        2017-11-02,54.53,55.1,54.27,55.04,0.29,54.89,35326.0,95525.0
        ...
    """

    logging.basicConfig(level=logging.DEBUG, format='%(filename)s: %(message)s')
    logger = logging

    def __init__(self, paths, prefix=[]):
        """
        Initialize with a list of paths for which each call to get_data() tries to open
        csv file directly in paths. 
        
        For example calling get_data() with ticker 'SPY' using paths ['data/stocks', 'data/etf'] 
        will try to open 'data/stocks/SPY.csv' and 'data/etf/SPY.csv' with first found file 
        being used.
        
        :param list paths: A list of paths containing csv-files  
        :param list prefix: An optional list of prefixes e.g. ['NYS','NYSF'] will find file  
        "NYS_AAPL.csv" or "NYSF_AAPL.csv"
        """
        self.paths = paths
        self.prefix = prefix

    def _get_data_internal(self, ticker, from_date, to_date, timeframe, transform, **kwargs):
        for path in self.paths:
            filenames = ["{}/{}_{}.{}".format(path, prefix, ticker,'csv') for prefix in self.prefix]
            filenames.append("{}/{}.{}".format(path,ticker,'csv'))

            for filename in filenames:
                self.logger.debug("Trying '{}'".format(filename))
                if os.path.exists(filename):
                    with open(filename) as f:
                        df = pd.read_csv(f)
                        df = df.set_index(pd.DatetimeIndex(df['Date'])).sort_index()
                        self.logger.info("{}, {:d} rows ({} to {})"
                                          .format(f.name, len(df), df.index[0], df.index[-1]))
                        return self._post_process(df, ticker, from_date, to_date, timeframe,
                                                  transform, **kwargs)

        self.logger.info("{} not found in {}".format(ticker, self.paths))


    def add_quotes(self, data, ticker):
        """
        Quotes are not available...         
        """
        return data


if __name__ == '__main__':
    home = os.environ['USERPROFILE']
    paths = [f"{home}/Dropbox/csv/"]
    provider = CsvFileDataProvider(paths)
    dailys = provider.get_data(['OMXS30 F18-OMF_1 Minute'], '2010-01-01', '2016-12-31', timeframe='1min', transform='5min')
    print(dailys[0])
    # print(dailys[1].tail())
