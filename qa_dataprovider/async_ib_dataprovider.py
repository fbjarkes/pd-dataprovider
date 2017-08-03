#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import logging
from datetime import datetime, timedelta
import asyncio

import dateutil.parser as dp
import numpy as np
import pandas as pd
import pandas_datareader
import pandas_datareader.data as web
import requests_cache
from ib_insync import IB, Stock, Index, Forex, Future, CFD, Commodity, util
from pytz import timezone


from qa_dataprovider.generic_dataprovider import GenericDataProvider


class AsyncIBDataProvider(GenericDataProvider):

    CLIENT_ID = 0

    logging.basicConfig(level=logging.DEBUG, format='%(filename)s: %(message)s')
    logger = logging

    @staticmethod
    def get_unique_id():
        AsyncIBDataProvider.CLIENT_ID += 1
        return AsyncIBDataProvider.CLIENT_ID

    def __init__(self, host: str='127.0.0.1', port: int= 7496, timeout: int=10):
        self.client_id = AsyncIBDataProvider.get_unique_id()
        self.port = port
        self.host = host
        self.timeout = timeout

    def get_data(self, tickers, from_date, to_date, timeframe='day', max_workers=1) -> [pd.DataFrame]:
        self.ib = IB()
        self.ib.connect(self.host, self.port, clientId=self.client_id, timeout=self.timeout)

        df_list = []

        for ticker in tickers:
            df_list.append(self._get_data_internal(ticker, from_date, to_date, timeframe))

        self.ib.disconnect()
        return df_list

    def _get_data_internal(self, ticker: str, from_date: str, to_date: str, timeframe: str)\
            -> pd.DataFrame:

        if timeframe == 'day':

            from_dt = datetime.strptime(from_date, "%Y-%m-%d")
            to_dt = datetime.strptime(to_date, "%Y-%m-%d")
            days = (to_dt-from_dt).days # TODO: Calendar days or trading days?

            if days > 365:
                self.logger.warning(F"Historical data is limited to 365 Days. "
                                    F"Only requesting for year '{from_dt.year}'")
                days = 365
                to_dt = datetime(from_dt.year, 12, 31, 23, 59, 59)


            if to_dt > datetime.today():
                to_dt = None

            contract = Stock(ticker,'ARCA','USD')

            whatToShow = 'MIDPOINT' if isinstance(contract, (Forex, CFD, Commodity)) else 'TRADES'
            #bars = self.ib.reqDailyBars(contract, 2016)
            bars = self.ib.reqHistoricalData(contract, endDateTime=to_dt, durationStr=F"{days} D",
                barSizeSetting="1 day",
                whatToShow=whatToShow,
                useRTH=True,
                formatDate=1)

            df = self.__to_dataframe(bars)
            df = self._post_process(df, ticker.upper(), from_date, to_date, timeframe)

            return df
        else:
            raise Exception("Not implemented")

    def __to_dataframe(self, bars):
        data = [{'Date':pd.to_datetime(b.date), 'Open': b.open, 'High': b.high, 'Low': b.low,
                      'Close':b.close, 'Volume':b.volume} for b in bars]
        df = pd.DataFrame(data).set_index('Date')
        return df

    def add_quotes(self, data, ticker):
        return data

if __name__ == '__main__':
    ib = AsyncIBDataProvider()
    dailys = ib.get_data(['OMXS30'], '2017-01-01', '2017-05-31', max_workers=5)

    #dailys = provider.get_data(['DIS', 'KO', 'BA', 'MSFT'], '2010-01-01', '2016-12-31',
    #                           max_workers=5, timeframe='week')

    for df in dailys:
        print(df.head())
        print(df.tail())
    #print(dailys[0].tail())
    #print(dailys[1].tail())
