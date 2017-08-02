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

    def __init__(self, host: str="127.0.0.1", port: int= 7496, timeout: int=10):
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

        if timeframe != "day":
            raise Exception("Not implemented")

        self.logger.warning("Historical data is limited to 365 Days")
        #contract = Stock('OMXS30', 'OMS', 'SEK')
        #contract = Index("AD-NSD", "NASDAQ","USD")
        #contract = Index("COMP", "NASDAQ", "USD")
        #contract = Index('OMXS30', 'OMS', 'SEK')
        #contract = Stock('HM.B', 'SFB', 'SEK')
        #contract = Stock('BA', 'NYSE', 'USD')
        #contract = Index('TICK-NASD', 'NASDAQ', 'USD')
        #contract = Forex(pair="EURGBP")
        #contract = Forex(pair="USDSEK")
        #contract = Forex(pair="USDJPY")
        #contract = Future("ES","201709",exchange="GLOBEX")
        #contract = Index('N225', 'OSE.JPN', 'JPY')
        #contract = Future("OMXS30","201708",exchange="OMS")
        #contract = Forex(pair="USDJPY")

        contract = Stock(ticker,"ARCA","USD")
        from_dt = datetime.strptime(from_date, '%Y-%m-%d')
        to_dt = datetime.strptime(to_date, '%Y-%m-%d')
        days = (to_dt-from_dt).days
        whatToShow = 'MIDPOINT' if isinstance(
            contract, (Forex, CFD, Commodity)) else 'TRADES'
        #date = self.ib.reqHeadTimeStamp(contract, whatToShow=whatToShow, useRTH=True)
        #print("Data from:", date)
        print("Before bars")
        bars = self.ib.reqDailyBars(contract, 2017)
        # bars = self.ib.reqHistoricalData(
        #     contract,
        #     endDateTime=to_date,
        #     durationStr=F'{days} D',
        #     barSizeSetting='1 day',
        #     whatToShow=whatToShow,
        #     useRTH=True,
        #     formatDate=1)
        #print("after")
        #print(tmp)
        print("after bars")
        return util.df(bars)

    def add_quotes(self, data, ticker):
        pass

if __name__ == '__main__':
    ib = AsyncIBDataProvider()
    dailys = ib.get_data(['XLE', 'SPY','XPH'], '2010-01-01', '2016-12-31', max_workers=5)

    #dailys = provider.get_data(['DIS', 'KO', 'BA', 'MSFT'], '2010-01-01', '2016-12-31',
    #                           max_workers=5, timeframe='week')

    for df in dailys:
        print(df.head())
        print(df.tail())
    #print(dailys[0].tail())
    #print(dailys[1].tail())
