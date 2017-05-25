#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import logging as logger
from abc import ABCMeta, abstractmethod
import concurrent.futures
from functools import reduce

import pandas as pd

from qa_dataprovider.validator import Validator


class GenericDataProvider(metaclass=ABCMeta):

    logger.basicConfig(level=logger.INFO, format='%(filename)s: %(message)s')
    validator = Validator()

    @abstractmethod
    def _get_data_internal(self, ticker, from_date, to_date, timeframe):
        pass

    @abstractmethod
    def _add_quotes(self, data, ticker):
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
                    logger.warning("Skipping {ticker}: {error}".format(ticker=ticker, error=exc))
                else:
                    dataframes.append(data)

        self.errors = len(dataframes) - len(tickers)

        return dataframes

    def _post_process(self, data, ticker,from_date, to_date, timeframe):
        func_args = {
            'ticker': ticker,
            'timeframe': timeframe,
            'from': from_date,
            'to': to_date
        }
        # First validate, then transform (if necessary), then add metadata
        funcs = [self._validate, self._transform, self._add_meta_data]
        return reduce((lambda result, func: func(result, func_args)), funcs, data)

    def _transform(self, data, kwargs):
        if kwargs['timeframe'] == 'week':
            data = self._transform_week(data)

        # TODO: monthly
        if kwargs['timeframe'] == 'month':
            pass

        return data

    def _validate(self, data, kwargs):
        self.validator.validate_nan(data, kwargs['ticker'])
        self.validator.validate_dates(data, kwargs['ticker'], kwargs['from'], kwargs['to'])
        return data

    def _add_meta_data(self, data, kwargs):
        data = self._add_quotes(data, kwargs['ticker'])

        if kwargs['timeframe'] == 'day':
            data = self.__add_trading_days(data, "Day")

        data['Ticker'] = kwargs['ticker']

        return data

    def _transform_week(self, data):
        # From: http://blog.yhat.com/posts/stock-data-python.html
        transdat = data.loc[:, ["Open", "High", "Low", "Close", 'Volume']]

        transdat["week"] = pd.to_datetime(transdat.index).map(
            lambda x: x.isocalendar()[1])  # Identify weeks
        transdat["year"] = pd.to_datetime(transdat.index).map(lambda x: x.isocalendar()[0])
        # Group by year and other appropriate variable
        grouped = transdat.groupby(list(set(["year", "week"])))
        dataframes = pd.DataFrame({"Open": [], "High": [], "Low": [], "Close": [], "Volume": []})
        for name, group in grouped:
            df = pd.DataFrame(
                {"Open": group.iloc[0, 0], "High": max(group.High), "Low": min(group.Low),
                 "Close": group.iloc[-1, 3], "Volume": group.Volume.sum()},
                index=[group.index[0]])
            dataframes = dataframes.append(df)
        sorted = dataframes.sort_index()

        return sorted


    def __add_trading_days(self, df, column_name):
        groups = df.groupby(df.index.year)
        for group in groups:
            yearly = group[1]
            day = 1
            for index, row in yearly.iterrows():
                df.loc[index, column_name] = day
                day += 1

        return df


