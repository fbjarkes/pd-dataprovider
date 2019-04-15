#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import sys, traceback
import logging as logger
from abc import ABCMeta, abstractmethod
import concurrent.futures
from functools import reduce
import pandas as pd

from qa_dataprovider.post_processor import PostProcessor
from qa_dataprovider.validator import Validator

class GenericDataProvider(metaclass=ABCMeta):

    post_processor = PostProcessor()

    errors = 0

    @abstractmethod
    def _get_data_internal(self, ticker: str, from_date: str, to_date: str, timeframe: str) -> \
            pd.DataFrame:
        pass

    @abstractmethod
    def add_quotes(self, data, ticker):
        """
        If quotes are available by this provider, append quotes row to data.
        
        :param data dataframe: pd.DataFrame
        :param ticker string: The ticker, eg. 'SPY'
        :return: pd.DataFrame
        """
        pass

    def _finish(self):
        """        
        Do any post data fetching activities, e.g. disconnect, exit async loop, etc.
        """
        pass

    def get_data(self, tickers, from_date, to_date, timeframe='day', transform='day',
                 max_workers=1, **kwargs):
        """
        Fetch a dataframe for each ticker, using the internal method with multiple threads
        
        :param list tickers: A list of tickers
        :param string from_date: Start date, e.g. '2016-01-01'
        :param string to_date: End date, e.g. '2017-01-01'
        :param int workers: Number of threads
        :return: List with dataframes
        """
        dataframes = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._get_data_internal,
                                ticker, from_date, to_date, timeframe, transform, **kwargs): ticker
                for
                ticker in tickers}

            for future in concurrent.futures.as_completed(futures):
                ticker = futures[future]
                try:
                    data = future.result()
                except Exception as exc:
                    traceback.print_exc(file=sys.stderr)
                    logger.debug("",exc_info=True)
                    logger.warning("Skipping {}: error message: {}".format(ticker, exc))
                else:
                    if data is not None:
                        dataframes.append(data)

        self.errors = len(dataframes) - len(tickers)

        self._finish()

        return dataframes

    def _post_process(self, data, ticker, from_date, to_date, timeframe, transform, **kwargs):
        func_args = {
            'ticker': ticker,
            'timeframe': timeframe,
            'transform': transform,
            'from': from_date,
            'to': to_date,
            'provider': self,
        }
        func_args.update(**kwargs)
        # Post process data in this order:
        funcs = [self.post_processor.filter_dates,
                 self.post_processor.filter_rth,
                 self.post_processor.validate,
                 self.post_processor.add_quotes,
                 self.post_processor.transform_timeframe,
                 self.post_processor.add_trading_days,
                 self.post_processor.add_meta_data
                 ]

        return reduce((lambda result, func: func(result, func_args)), funcs, data)



