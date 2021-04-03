import logging
import pandas as pd
import numpy as np

from qa_dataprovider.utils import log_helper
from qa_dataprovider.utils.validator import Validator


class PostProcessor:

    logger = logging.getLogger(__name__)
    validator = Validator()
    ta = {}

    def __init__(self, verbose, **kwargs):
        log_helper.init_logging([self.logger], verbose)
        self.ta = kwargs['ta'] if 'ta' in kwargs else {}

    def filter_dates(self, df, kwargs):
        from_date = kwargs['from']
        to_date = kwargs['to']
        if from_date and to_date:
            return pd.DataFrame(df[from_date:to_date])
        return  df

    def filter_rth(self, data, kwargs):
        if kwargs['timeframe'] not in ['day', 'week', 'month'] and 'rth_only' in kwargs and kwargs['rth_only']:
            self.logger.debug('Filtering for RTH only')
            return pd.DataFrame(data.between_time('09:30', '16:00'))
        else:
            return data

    def transform_timeframe(self, data, kwargs):
        if kwargs['timeframe'] == kwargs['transform']:
            return data # Let it pass regardless of timeframe

        if kwargs['timeframe'] == 'day':
            if kwargs['transform'] == 'week':
                data = self._transform_week(data)
            if kwargs['transform'] == 'month':
                data = self._transform_month(data)
            if "D" in kwargs['transform'] and kwargs['transform'] != "1D":
                data = self._transform_day(data, kwargs['transform'][:-1])
        elif kwargs['timeframe'] == '5min':
            if kwargs['transform'] == '10min':
                data = self._transform_min(10, data)
            if kwargs['transform'] == '15min':
                data = self._transform_min(15, data)
            if kwargs['transform'] == '30min':
                data = self._transform_min(30, data)
            if kwargs['transform'] == '60min':
                data = self._transform_hour(data)
            if kwargs['transform'] == 'day':
                data = self._transform_day(data, 1)
            if kwargs['transform'] == 'week':
                daily = self._transform_day(data, 1)
                data = self._transform_week(daily)
        elif kwargs['timeframe'] == '1min':
            if kwargs['transform'] == '5min':
                data = self._transform_min(5, data)
            if kwargs['transform'] == '60min':
                data = self._transform_hour(data)
        elif kwargs['timeframe'] == '60min':
            if kwargs['transform'] == '240min':
                data = self._transform_min(240, data)
        elif kwargs['timeframe'] == '15min':
            if kwargs['transform'] == '30min':
                data = self._transform_min(30, data)
            elif kwargs['transform'] == '60min':
                data = self._transform_min(60, data)
            elif kwargs['transform'] == '240min':
                data = self._transform_min(240, data)
            elif kwargs['transform'] == 'day':
                data = self._transform_day(data, 1)
            elif kwargs['transform'] == 'week':
                daily = self._transform_day(data, 1)
                data = self._transform_week(daily)
        else:
            raise Exception(
                f"NOT IMPLEMENTED: transform '{kwargs['timeframe']}' to '{kwargs['transform']}'")
        return data

    def validate(self, data, kwargs):
        self.validator.validate_nan(data, kwargs['ticker'])
        return data

    def faulty_values(self, data, kwargs):
        """
            E.g when Open or Low is 0.0 (faulty), screwing up calculations
        """
        pass

    def add_quotes(self, data, kwargs):
        data = kwargs['provider'].add_quotes(data, kwargs['ticker'])
        return data

    def add_meta_data(self, df, kwargs):
        #df['Ticker'] = kwargs['ticker']
        df.symbol = kwargs['ticker']
        return df

    def add_vwap(self, df, kwargs):
        pass
        # For each session:
        #   p = (df['High'] + df['Low'] + df['Close']) / 3
        #   q = df

    def add_linearity(self, df: pd.DataFrame, kwargs):
        """
        Add Columns: ConsOpen, ConsHigh, ConsLow, ConsClose
        """
        if 'linearity' not in self.ta:
            return df

        df['ConsOpen'], df['ConsHigh'], df['ConsLow'], df['ConsClose'] = 0, 0, 0, 0
        n_cols = len(df.columns)
        co_idx, ch_idx, cl_idx, cc_idx = n_cols-4, n_cols-3, n_cols-2, n_cols-1

        def func(i, j, col1, col2):
            if df.iloc[i][col1] >= df.iloc[i-1][col1]:
                if df.iloc[i-1][col2] >= 0:
                    df.iat[i, j] = df.iloc[i-1][col2] + 1
                else:
                    df.iat[i, j] = 1
            else:
                if df.iloc[i - 1][col2] <= 0:
                    df.iat[i, j] = df.iloc[i-1][col2] - 1
                else:
                    df.iat[i, j] = -1

        #for i in range(1, len(df)):
        period = 10
        for i in range(1, period):
            idx = len(df) - period + i
            func(idx, co_idx, 'Open', 'ConsOpen')
            func(idx, ch_idx, 'High', 'ConsHigh')
            func(idx, cl_idx, 'Low', 'ConsLow')
            func(idx, cc_idx,'Close', 'ConsClose')
            # if df.iloc[i].Open >= df.iloc[i-1].Open:
            #     if df.iloc[i-1].ConsOpen >= 0:
            #         df.iat[i, co_idx] = df.iloc[i-1].ConsOpen + 1
            #     else:
            #         df.iat[i, co_idx] = 1
            # else:
            #     if df.iloc[i - 1].ConsOpen <= 0:
            #         df.iat[i, co_idx] = df.iloc[i-1].ConsOpen - 1
            #     else:
            #         df.iat[i, co_idx] = -1
        return df



    def fill_na(self, df, kwargs):
        df.fillna(method='ffill', inplace=True)
        return df

    def _transform_week(self, data):
        self.logger.debug(f"Transforming to 1 week")
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

    def _transform_hour(self, data):
        self.logger.debug(f"Transforming to 1H")
        conversion = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
        # TODO: remove 16:00 bar? Official close is Open of 16:00 bar?
        # TODO: if RTH modify first datetime index each day to '9:30' (data is correct but looks odd)
        resampled = data.resample('60Min').agg(conversion)
        return resampled.dropna()

    def _transform_day(self, data, transform):
        self.logger.debug(f"Transforming to 1D")
        conversion = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
        resampled = data.resample(f"{transform}D").agg(conversion)
        return resampled.dropna()

    def _transform_min(self, to_tf, data):
        self.logger.debug(f"Transforming to {to_tf}")
        conversion = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
        resampled = data.resample(f"{to_tf}Min").agg(conversion)
        return resampled.dropna()

    def _transform_month(self, data):
        self.logger.debug(f"Transforming to 1M")
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
        if kwargs['transform'] == 'day':
            data = self._add_trading_days(data, "Day")
        return data

    def _add_trading_days(self, df, column_name):
        groups = df.groupby(df.index.year)
        for group in groups:
            yearly = group[1]
            day = 1
            for index, row in yearly.iterrows():
                df.loc[index, column_name] = day
                day += 1
        return df
