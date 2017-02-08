#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-

from datetime import datetime, timedelta
import logging as logger
import concurrent.futures

import dateutil.parser as dp
from pytz import timezone
import numpy as np
import pandas as pd
import requests_cache
import pandas_datareader.data as web
import pandas_datareader

class DataProvider:
    """
    """

    logger.basicConfig(level=logger.INFO, format='%(filename)s: %(message)s')

    session = None

    def __init__(self, quote):
        # TODO: if self.quote then request quotes using 50 tickers chunks (instead of one request per ticker)
        self.quote = quote
        self.errors = 0

    def get_today_est(self):
        """
        Get the current EST day e.g.  '20170201'
        """
        localized_utc = timezone("Europe/London").localize(datetime.utcnow())
        return localized_utc.astimezone(timezone("US/Eastern"))

    def get_quote(self, ticker, provider):
        """
        Return dataframe with only latest traded price.

        :param ticker:
        :param today_est:
        :param provider:
        :return: pandas DataFrame
        """
        open = np.nan
        high = np.nan
        low = np.nan
        close = np.nan

        if provider == 'google':
            resp = pandas_datareader.get_quote_google([ticker])
            close = resp.loc[ticker]['last']
            last_dt = resp.loc[ticker]['time']

        if provider == 'yahoo':
            resp = pandas_datareader.get_quote_yahoo([ticker])
            close = resp.loc[ticker]['last']
            last_time = resp.loc[ticker]['time']

            # TODO: is there an easier way to construct correct datetime from only '8pm'?
            # NOTE: this will work as long as 'utcnow()' is the same day as EST
            last_dt = datetime.combine(self.get_today_est(), dp.parse(last_time).time())

        df = pd.DataFrame({"Ticker": ticker, "Open": [open], "High": [high], "Low": [low], "Close": [close]})
        df = df.set_index(pd.DatetimeIndex([last_dt.strftime('%Y%m%d')]))

        return df

    def _add_quote(self, historical, ticker, provider):
        quote = self.get_quote(ticker, provider)
        quote_dt = quote.index[0].strftime("%Y-%m-%d")

        if quote_dt in historical.index:
            logger.warning("Skipping quote for {0} since {1} is in historical".format(ticker, quote_dt))
        else:
            historical = historical.append(quote)

        return historical


    def get_data_parallel(self, tickers, from_date, to_date, max_workers=5, timeframe='day', provider='google'):
        """
        Download historical data in parallel
        :param tickers:
        :param from_date: e.g. '2016-01-01'
        :param to_date: e.g. '2017-01-01'
        :param workers:
        :param timeframe:
        :param provider:
        :return:
        """

        dataframes = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.get_data, ticker, from_date, to_date, timeframe, provider): ticker for
                       ticker in tickers}
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

    def get_data(self, ticker, from_date, to_date, timeframe='day', provider='google'):
        """
        Note this will only get historical data.
        """
        logger.info("%s: %s to %s, provider=%s" % (ticker, from_date, to_date, provider))

        start = datetime.strptime(from_date, "%Y-%m-%d")
        end = datetime.strptime(to_date, "%Y-%m-%d")
        data = web.DataReader(ticker, provider, start=start, end=end, session=self.session, pause=1)

        # Transformation logic from: http://blog.yhat.com/posts/stock-data-python.html
        transdat = data.loc[:, ["Open", "High", "Low", "Close"]]
        if timeframe == 'week':
            transdat["week"] = pd.to_datetime(transdat.index).map(lambda x: x.isocalendar()[1])  # Identify weeks
            transdat["year"] = pd.to_datetime(transdat.index).map(lambda x: x.isocalendar()[0])

            grouped = transdat.groupby(list(set(["year", "week"])))  # Group by year and other appropriate variable
            dataframes = pd.DataFrame({"Open": [], "High": [], "Low": [], "Close": []})
            for name, group in grouped:
                df = pd.DataFrame({"Open": group.iloc[0, 0], "High": max(group.High), "Low": min(group.Low),
                                   "Close": group.iloc[-1, 3]}, index=[group.index[0]])
                dataframes = dataframes.append(df)

            sorted = dataframes.sort_index()
            historical = self.__add_ticker(ticker, sorted)

        else:
            historical = self.__add_ticker(ticker, transdat)

        if self.quote:
            historical = self._add_quote(historical, ticker, provider)

        return historical

    def __add_ticker(self, ticker, df):
        df['Ticker'] = ticker
        return df


class CachedDataProvider(DataProvider):
    """
    A sqlite cache supported version of WebDataprovider
    """

    def __init__(self, quote=False, cache_name='cache', expire_days=3):
        super().__init__(quote)

        expire_after = (None if expire_days is (None or 0) else timedelta(days=expire_days))
        self.session = requests_cache.CachedSession(cache_name=cache_name, backend='sqlite',
                                                    expire_after=expire_after)
        logger.info("Using cache '{0}' with {1} items. Expires ?".format(cache_name, len(self.session.cache.responses)))

class AWSDataProvider(DataProvider):
    pass

def main():
    provider = DataProvider(quote=True)
    #print(provider.get_data_parallel(['spy','aapl'],from_date='2016-12-01', to_date='2016-12-31'))
    print(provider.get_quote('SPY','yahoo'))

if __name__ == '__main__':
    main()