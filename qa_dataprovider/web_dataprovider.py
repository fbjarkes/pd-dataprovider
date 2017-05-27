#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import logging as logger
from datetime import datetime, timedelta


import dateutil.parser as dp
import numpy as np
import pandas as pd
import pandas_datareader
import pandas_datareader.data as web
import requests_cache
from pytz import timezone

from qa_dataprovider.validator import Validator
from qa_dataprovider.generic_dataprovider import GenericDataProvider


class WebDataProvider(GenericDataProvider):

    urllib3_logger = logger.getLogger('urllib3').setLevel(logger.WARNING)
    session = None

    def __init__(self, provider, quotes=False, **kwargs):
        # TODO: if self.quote then request quotes using 50 tickers chunks (instead of one request per ticker)
        self.provider = provider
        self.errors = 0
        self.quotes = quotes


    #TODO: memoize call?
    def _get_data_internal(self, ticker, from_date, to_date, timeframe):
        ticker = ticker.upper()
        logger.info("%s: %s to %s, provider=%s" % (ticker, from_date, to_date, self.provider))

        start = datetime.strptime(from_date, "%Y-%m-%d")
        end = datetime.strptime(to_date, "%Y-%m-%d")
        data = web.DataReader(ticker, self.provider, start=start, end=end, session=self.session, pause=1)

        return self._post_process(data, ticker, from_date, to_date, timeframe)

    def _get_today_est(self):
        """
        Get the current EST day e.g.  '20170201'
        """
        #TODO: just set once in __init__
        localized_utc = timezone("Europe/London").localize(datetime.utcnow())
        return localized_utc.astimezone(timezone("US/Eastern"))


    def __get_quote(self, ticker):
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

        if self.provider == 'google':
            resp = pandas_datareader.get_quote_google([ticker])
            close = resp.loc[ticker]['last']
            last_dt = resp.loc[ticker]['time']

        if self.provider == 'yahoo':
            resp = pandas_datareader.get_quote_yahoo([ticker])
            close = resp.loc[ticker]['last']
            last_time = resp.loc[ticker]['time']

            # TODO: is there an easier way to construct correct datetime from only '8pm'?
            # NOTE: this will work as long as 'utcnow()' is the same day as EST
            last_dt = datetime.combine(self._get_today_est(), dp.parse(last_time).time())

        df = pd.DataFrame(
            {"Ticker": ticker, "Open": [open], "High": [high], "Low": [low], "Close": [close]})
        df = df.set_index(pd.DatetimeIndex([last_dt.strftime('%Y%m%d')]))

        return df

    def _add_quotes(self, data, ticker):
        if self.quotes:
            quote = self.__get_quote(ticker)
            quote_dt = quote.index[0].strftime("%Y-%m-%d")

            if quote_dt in data.index:
                logger.warning(
                    "Skipping quote for {0} since {1} is in historical".format(ticker, quote_dt))
            else:
                data = data.append(quote)

        return data




class CachedWebDataProvider(WebDataProvider):

    CACHE_NAME = 'cache'

    def __init__(self, provider, quotes=False, cache_name=CACHE_NAME, expire_days=3, **kwargs):
        super().__init__(provider, quotes, **kwargs)
        expire_after = (None if expire_days is (None or 0) else timedelta(days=expire_days))
        self.session = requests_cache.CachedSession(cache_name=cache_name, backend='sqlite',
                                                    expire_after=expire_after)
        logger.info("Using cache '{0}' with {1} items.".format(cache_name,
                                                               len(self.session.cache.responses)))



def main():
    provider = CachedWebDataProvider('google')
    print(provider.get_data(['SPY','QQQ','TLT','GLD'], from_date='2010-01-01',
                            to_date='2016-12-31', max_workers=10))

if __name__ == '__main__':
    main()
