import logging
import pandas as pd

from qa_dataprovider.utils import log_helper
from qa_dataprovider.utils.validator import Validator


class PostProcessor:

    logger = logging.getLogger(__name__)
    validator = Validator()

    def __init__(self, verbose):
        log_helper.init_logging([self.logger], verbose)

    def filter_dates(self, df, kwargs):
        from_date = kwargs['from']
        to_date = kwargs['to']
        if from_date and to_date:
            return pd.DataFrame(df[from_date:to_date])
        return  df

    def filter_rth(self, data, kwargs):
        if kwargs['timeframe'] not in ['day', 'week', 'month'] and 'rth_only' in kwargs and kwargs['rth_only']:
            self.logger.debug('Filtering for RTH only')
            return data.between_time('09:30', '16:00')
        else:
            return data

    def transform_timeframe(self, data, kwargs):
        if kwargs['timeframe'] == 'day':
            if kwargs['transform'] == 'week':
                data = self._transform_week(data)
            if kwargs['transform'] == 'month':
                data = self._transform_month(data)
            if "D" in kwargs['transform'] and kwargs['transform'] != "1D":
                data = self._transform_day(data, kwargs['transform'][:-1])
        elif kwargs['timeframe'] == '5min':
            if kwargs['transform'] == '60min':
                data = self._transform_hour(data)
        elif kwargs['timeframe'] == kwargs['transform']:
            pass # Let it pass regardless of timeframe
        elif kwargs['timeframe'] == '1min':
            if kwargs['transform'] == '5min':
                data = self._transform_min(5, data)
            if kwargs['transform'] == '60min':
                data = self._transform_hour(data)
        elif kwargs['timeframe'] == '60min':
            if kwargs['transform'] == '240min':
                data = self._transform_min(240, data)
        else:
            raise Exception(
                f"NOT IMPLEMENTED: transform '{kwargs['timeframe']}' to '{kwargs['transform']}'")
        return data

    def validate(self, data, kwargs):
        self.validator.validate_nan(data, kwargs['ticker'])
        return data

    def add_quotes(self, data, kwargs):
        data = kwargs['provider'].add_quotes(data, kwargs['ticker'])
        return data

    def add_meta_data(self, df, kwargs):
        df['Ticker'] = kwargs['ticker']
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
