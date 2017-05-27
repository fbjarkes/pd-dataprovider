#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import logging as logger
from abc import ABCMeta, abstractmethod
import concurrent.futures
from functools import reduce

from qa_dataprovider.post_processor import PostProcessor
from qa_dataprovider.validator import Validator


class GenericDataProvider(metaclass=ABCMeta):

    logger.basicConfig(level=logger.INFO, format='%(filename)s: %(message)s')
    post_processor = PostProcessor()

    @abstractmethod
    def _get_data_internal(self, ticker, from_date, to_date, timeframe):
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

    #TODO: should work with intraday data also with exactly this interface?
    def get_data(self, tickers, from_date, to_date, timeframe='day', max_workers=1):
        """
        Fetch a dataframe for each ticker, using the internal method with multiple threads
        
        :param list tickers: A list of tickers
        :param string from_date: Start date, e.g. '2016-01-01'
        :param string to_date: End date, e.g. '2017-01-01'
        :param int workers: Number of threads
        :return:
        """
        dataframes = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._get_data_internal,
                                ticker, from_date, to_date, timeframe): ticker for ticker in
                tickers}

            for future in concurrent.futures.as_completed(futures):
                ticker = futures[future]
                try:
                    data = future.result()
                except Exception as exc:
                    logger.warning("Skipping {}: error message: {}".format(ticker, exc))
                else:
                    dataframes.append(data)

        self.errors = len(dataframes) - len(tickers)

        return dataframes

    def _post_process(self, data, ticker,from_date, to_date, timeframe):
        func_args = {
            'ticker': ticker,
            'timeframe': timeframe,
            'from': from_date,
            'to': to_date,
            'provider': self,
        }

        # Post process data in this order:
        funcs = [self.post_processor.filter_dates,
                 self.post_processor.validate,
                 self.post_processor.add_quotes,
                 self.post_processor.transform_timeframe,
                 self.post_processor.add_trading_days
                 ]

        return reduce((lambda result, func: func(result, func_args)), funcs, data)



