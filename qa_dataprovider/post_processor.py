#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import logging as logger

import pandas as pd

from qa_dataprovider.validator import Validator


class PostProcessor:

    logger.basicConfig(level=logger.INFO, format='%(filename)s: %(message)s')
    validator = Validator()

    def filter_dates(self, data, kwargs):
        from_date = kwargs['from']
        to_date = kwargs['to']

        return pd.DataFrame(data[from_date:to_date])

    def filter_rth(self, data, kwargs):
        if 'rth' in kwargs:
            return data.between_time('09:30', '16:00')
        else:
            return data

    def transform_timeframe(self, data, kwargs):
        if kwargs['timeframe'] == 'day':
            if kwargs['transform'] == 'week':
                data = self._transform_week(data)

            if kwargs['transform'] == 'month':
                data = self._transform_month(data)
        elif kwargs['timeframe'] == kwargs['transform']:
            pass # Let it pass regardless of timeframe
        else:
            raise Exception(
                f"NOT IMPLEMENTED: transform '{kwargs['timeframe']}' to '{kwargs['transform']}'")
        return data

    def validate(self, data, kwargs):
        self.validator.validate_nan(data, kwargs['ticker'])
        self.validator.validate_dates(data, kwargs['ticker'], kwargs['from'], kwargs['to'])
        self.validator.validate_timeframe(data, kwargs['timeframe'])
        return data

    def add_quotes(self, data, kwargs):
        data = kwargs['provider'].add_quotes(data, kwargs['ticker'])
        return data

    def add_meta_data(self, data, kwargs):
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

    def _transform_month(self, data):

        transdat = data.loc[:, ["Open", "High", "Low", "Close", 'Volume']]


        transdat["week"] = pd.to_datetime(transdat.index).map(lambda x: x.week)
        transdat["year"] = pd.to_datetime(transdat.index).map(lambda x: x.year)
        transdat["month"] = pd.to_datetime(transdat.index).map(lambda x: x.month)

        # Group by year and other appropriate variable
        grouped = transdat.groupby(list(set(["year", "month"])))
        dataframes = pd.DataFrame({"Open": [], "High": [], "Low": [], "Close": [], "Volume": []})
        for name, group in grouped:
            df = pd.DataFrame(
                {"Open": group.iloc[0, 0], "High": max(group.High), "Low": min(group.Low),
                 "Close": group.iloc[-1, 3], "Volume": group.Volume.sum()},
                index=[group.index[0]])
            dataframes = dataframes.append(df)
        sorted = dataframes.sort_index()

        return sorted

    def add_day_of_month(self, data, kwargs):
        pass #TODO

    def add_trading_days(self, data, kwargs):
        if kwargs['timeframe'] == 'day':
            data = self.__add_trading_days(data, "Day")
        return data

    def __add_trading_days(self, df, column_name):
        groups = df.groupby(df.index.year)
        for group in groups:
            yearly = group[1]
            day = 1
            for index, row in yearly.iterrows():
                df.loc[index, column_name] = day
                day += 1

        return df
