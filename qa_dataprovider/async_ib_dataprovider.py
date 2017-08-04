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
from ib_insync import IB, Stock, Index, Forex, Future, CFD, Commodity, util, Contract
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

    def __init__(self, host: str='127.0.0.1', port: int= 7496, timeout: int=60):
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
            bars = self.__get_daily(from_date, ticker, to_date)
            df = self.__to_dataframe(bars)
            df = self._post_process(df, ticker.upper(), from_date, to_date, timeframe)

            return df
        else:
            raise Exception("Not implemented")

    @staticmethod
    def exctract_symbol(ticker: str, type: str='STK', exchange: str='ARCA',
                        currency: str= 'USD', expire='') -> tuple:

        if ticker.count('-') == 3:
            symbol, type, exchange, currency = ticker.split('-')
            if type.isdigit():
                expire = type
                type = "FUT"

        elif ticker.count('-') == 2:
            if ticker.find('CASH') > -1:
                symbol, currency, exchange = ticker.split('-')
                symbol = symbol.replace('.','')
                type = 'FX'
            else:
                a, b, c = ticker.split('-')
                if b.isdigit():
                    type = 'FUT'
                    symbol = a
                    exchange = c
                    expire = b
                else:
                    symbol = a
                    exchange = b
                    currency = c

        elif ticker.count('-') == 1:
            symbol, exchange = ticker.split('-')
        else:
            symbol = ticker

        return type, symbol, exchange, currency, expire

    @staticmethod
    def parse_contract(ticker):
        """
        Backtrader contract specification (https://www.backtrader.com/docu/live/ib/ib.html):
        
        TICKER # Stock type and SMART exchange
        
        TICKER-STK # Stock and SMART exchange
        
        TICKER-STK-EXCHANGE # Stock
        
        TICKER-STK-EXCHANGE-CURRENCY # Stock
        
        TICKER-CFD # CFD and SMART exchange
        
        TICKER-CFD-EXCHANGE # CFD
        
        TICKER-CDF-EXCHANGE-CURRENCY # Stock
        
        TICKER-IND-EXCHANGE # Index
        
        TICKER-IND-EXCHANGE-CURRENCY # Index
        
        TICKER-YYYYMM-EXCHANGE # Future
        
        TICKER-YYYYMM-EXCHANGE-CURRENCY # Future
        
        TICKER-YYYYMM-EXCHANGE-CURRENCY-MULT # Future
        
        TICKER-FUT-EXCHANGE-CURRENCY-YYYYMM-MULT # Future
        
        TICKER-YYYYMM-EXCHANGE-CURRENCY-STRIKE-RIGHT # FOP
        
        TICKER-YYYYMM-EXCHANGE-CURRENCY-STRIKE-RIGHT-MULT # FOP
        
        TICKER-FOP-EXCHANGE-CURRENCY-YYYYMM-STRIKE-RIGHT # FOP
        
        TICKER-FOP-EXCHANGE-CURRENCY-YYYYMM-STRIKE-RIGHT-MULT # FOP
        
        CUR1.CUR2-CASH-IDEALPRO # Forex
        
        TICKER-YYYYMMDD-EXCHANGE-CURRENCY-STRIKE-RIGHT # OPT
        
        TICKER-YYYYMMDD-EXCHANGE-CURRENCY-STRIKE-RIGHT-MULT # OPT
        
        TICKER-OPT-EXCHANGE-CURRENCY-YYYYMMDD-STRIKE-RIGHT # OPT
        
        TICKER-OPT-EXCHANGE-CURRENCY-YYYYMMDD-STRIKE-RIGHT-MULT # OPT
        
        :return: 
        """
        contract_type, symbol, exchange, currency, expire = AsyncIBDataProvider.exctract_symbol(
            ticker)

        if contract_type == 'FX':
            return Forex(pair=symbol)
        if contract_type == 'IND':
            return Index(symbol, exchange, currency)
        if contract_type == 'FUT':
            return Future(symbol, expire, exchange, currency=currency)
        else:
            return Stock(symbol, exchange, currency)


    # def __extract_symbol(self,  ticker: str, type: str='STK', exchange: str='ARCA',
    #                      currency_or_expire: str='USD') -> tuple:
    #     if ticker.count(':') == 3:
    #         type, exchange, symbol, currency_or_expire = ticker.split(':')
    #     elif ticker.count(':') == 2:
    #         exchange, symbol, currency_or_expire = ticker.split(':')
    #     elif ticker.count(':') == 1:
    #         exchange, symbol = ticker.split(':')
    #         if exchange == 'FX':
    #             type = 'FX'
    #
    #     else:
    #         symbol = ticker
    #     return type, symbol, exchange, currency_or_expire
    #
    # def __create_contract(self, ticker) -> Contract:
    #     type, symbol, exchange, currency_or_expire = self.__extract_symbol(ticker)
    #
    #     if type == 'FX':
    #         return Forex(pair=symbol)
    #     if type == 'IND':
    #         return Index(symbol, exchange, currency_or_expire)
    #     if type == 'FUT':
    #         return Future(symbol, currency_or_expire, exchange=exchange)
    #     else:
    #         return Stock(symbol, exchange, currency_or_expire)

    def __get_daily(self, from_date, ticker, to_date):
        from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        today = datetime.strptime(to_date, "%Y-%m-%d")
        to_dt = datetime(today.year, today.month, today.day, 23, 59, 59)
        days = (to_dt - from_dt).days
        if days > 365:
            self.logger.warning(F"Historical data is limited to 365 Days. "
                                F"Only requesting for year '{from_dt.year}'")
            days = 365
            to_dt = datetime(from_dt.year, 12, 31, 23, 59, 59)
        if to_dt > datetime.today():
            to_dt = None

        contract = AsyncIBDataProvider.parse_contract(ticker)
        whatToShow = 'MIDPOINT' if isinstance(contract, (Forex, CFD, Commodity)) else 'TRADES'
        # bars = self.ib.reqDailyBars(contract, 2016)
        bars = self.ib.reqHistoricalData(contract, endDateTime=to_dt, durationStr=F"{days} D",
                                         barSizeSetting="1 day",
                                         whatToShow=whatToShow,
                                         useRTH=True,
                                         formatDate=1)
        return bars

    def __to_dataframe(self, bars):
        data = [{'Date':pd.to_datetime(b.date), 'Open': b.open, 'High': b.high, 'Low': b.low,
                      'Close':b.close, 'Volume':b.volume} for b in bars]
        df = pd.DataFrame(data).set_index('Date')
        print(df.tail())
        return df

    def add_quotes(self, data, ticker):
        return data

if __name__ == '__main__':
    ib = AsyncIBDataProvider()
    #dailys = ib.get_data(['FUT:GLOBEX:ES:201709'],'2017-01-01', '2017-12-31', max_workers=5)
    #dailys = ib.get_data(['SFB:HM.B:SEK'], '2017-01-01', '2017-12-31', max_workers=5)
    dailys = ib.get_data(['IND:OMS:OMXS30:SEK','FX:USDJPY','SPY','FUT:GLOBEX:ES:201709','NYSE:BA','SFB:HM.B:SEK'],
                          '2017-01-01', '2017-12-31', max_workers=5)

    #dailys = provider.get_data(['DIS', 'KO', 'BA', 'MSFT'], '2010-01-01', '2016-12-31',
    #                           max_workers=5, timeframe='week')

    for df in dailys:
        print(df.head())
        print(df.tail())
    #print(dailys[0].tail())
    #print(dailys[1].tail())
