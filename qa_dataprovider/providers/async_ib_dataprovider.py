import logging
import traceback
from datetime import datetime, timedelta
import random

import pandas as pd
import sys
import pytz
from ib_insync import IB, Stock, Index, Forex, Future, CFD, Commodity, BarData

from qa_dataprovider.providers.generic_dataprovider import GenericDataProvider
from qa_dataprovider.objects import SymbolData


class AsyncIBDataProvider(GenericDataProvider):

    logger = logging.getLogger(__name__)

    def __init__(self, verbose: int, host: str, port: int, timeout: int, chunk_size: int, tz='America/New_York', **kwargs):
        super(AsyncIBDataProvider, self).__init__(self.logger, verbose, tz,chunk_size=chunk_size)
        self.port = port
        self.host = host
        self.timeout = timeout
        self.keep_alive = False
        if 'keep_alive' in kwargs:
            self.keep_alive = kwargs['keep_alive']
        self.ib = IB()

    def disconnect(self):
        self.ib.disconnect()

    def connect(self):
        id = int(random.uniform(1, 10000))
        self.logger.info(f"IBAsync: {self.host}:{self.port}, timeout={self.timeout}")
        self.ib.connect(self.host, self.port, clientId=id, timeout=self.timeout, readonly=True)

    def _initialize(self):
        if not self.ib.isConnected():
            self.connect()

    def _finish(self):
        if not self.keep_alive:
            self.disconnect()

    async def _get_data_internal_async(self, symbol_data: SymbolData, **kwargs) -> pd.DataFrame:
        return self._get_data_internal(symbol_data)

    def _get_data_internal(self, symbol_data: SymbolData) -> pd.DataFrame:
        self.logger.info(f"Getting symbol data: {symbol_data}")

        if symbol_data.timeframe == 'day':
            symbol, bars = self._get_daily(
                symbol_data.start, symbol_data.symbol, symbol_data.end)
            symbol = symbol_data.symbol.split('-')[0]
            dataframe = self._to_dataframe(bars)

        elif symbol_data.timeframe == '60min':
            if symbol_data.end is None:
                to_date = f"{(datetime.now()):%Y-%m-%d}"
            to_dt = datetime.strptime(symbol_data.end, "%Y-%m-%d")
            duration = '30 D'
            if symbol_data.start is not None:
                from_dt = datetime.strptime(symbol_data.start, "%Y-%m-%d")
                if (to_dt - from_dt).days >= 30:
                    from_date = f"{(to_dt - timedelta(days=30)):%Y-%m-%d}"
            else:
                from_date = f"{(to_dt - timedelta(days=30)):%Y-%m-%d}"

            symbol, bars = self._get_intraday(
                symbol_data.symbol, symbol_data.end, duration, '1 hour', symbol_data.rth_only)
            symbol = symbol_data.symbol.split('-')[0]
            dataframe = self._to_dataframe(bars, tz_fix=True)

        elif symbol_data.timeframe == '5min':
            if symbol_data.end is None:
                to_date = f"{(datetime.now()):%Y-%m-%d}"
            to_dt = datetime.strptime(symbol_data.end, "%Y-%m-%d")
            duration = '30 D'
            if symbol_data.start is not None:
                from_dt = datetime.strptime(symbol_data.start, "%Y-%m-%d")
                if (to_dt - from_dt).days >= 30:
                    from_date = f"{(to_dt - timedelta(days=30)):%Y-%m-%d}"
            else:
                from_date = f"{(to_dt - timedelta(days=30)):%Y-%m-%d}"

            symbol, bars = self._get_intraday(
                symbol_data.symbol, symbol_data.end, duration, '5 mins', symbol_data.rth_only)
            symbol = symbol_data.symbol.split('-')[0]
            dataframe = self._to_dataframe(bars, tz_fix=True)
        else:
            raise Exception(f"{symbol_data.timeframe} not implemented!")

        df = dataframe
        if dataframe.empty:
            self.logger.warning(f"Got empty df for {symbol_data}")
        else:
            df = self._post_process(dataframe, symbol, symbol_data.start,
                                    symbol_data.end, symbol_data.timeframe, symbol_data.transform)

        return df

    @staticmethod
    def exctract_symbol(ticker: str, type: str = 'STK', exchange: str = 'ARCA',
                        currency: str = 'USD', expire='', multiplier='') -> tuple:
        if ticker.count('-') == 4:
            symbol, type, exchange, currency, multiplier = ticker.split('-')
            if type.isdigit():
                expire = type
                type = "FUT"

        elif ticker.count('-') == 3:
            symbol, type, exchange, currency = ticker.split('-')
            if type.isdigit():
                expire = type
                type = "FUT"

        elif ticker.count('-') == 2:
            if ticker.find('CASH') > -1:
                symbol, currency, exchange = ticker.split('-')
                symbol = symbol.replace('.', '')
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

        return type, symbol, exchange, currency, expire, multiplier

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
        contract_type, symbol, exchange, currency, expire, multiplier = \
            AsyncIBDataProvider.exctract_symbol(ticker)

        if contract_type == 'FX':
            return Forex(pair=symbol)
        if contract_type == 'IND':
            return Index(symbol, exchange, currency)
        if contract_type == 'FUT':
            return Future(symbol, expire, exchange, currency=currency, multiplier=multiplier)
        else:
            return Stock(symbol, exchange, currency)

    def _get_intraday(self, ticker: str, to_date: str, duration: str,
                      barsize: str, rth_only: bool) -> (str, [BarData]):
        to_dt = datetime.strptime(f"{to_date} 11:59", '%Y-%m-%d %H:%M')
        contract = AsyncIBDataProvider.parse_contract(ticker)
        whatToShow = 'MIDPOINT' if isinstance(
            contract, (Forex, CFD, Commodity)) else 'TRADES'
        bars = self.ib.reqHistoricalData(contract, endDateTime=to_dt, durationStr=duration,
                                         barSizeSetting=barsize,
                                         whatToShow=whatToShow,
                                         useRTH=rth_only,
                                         formatDate=2)
        return contract.symbol, bars

    def _get_daily(self, from_date: str, ticker: str, to_date: str) -> (str, [BarData]):
        from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        today = datetime.strptime(to_date, "%Y-%m-%d")
        to_dt = datetime(today.year, today.month, today.day, 23, 59, 59)
        days = (to_dt - from_dt).days
        if days > 365:
            self.logger.warning(f"Historical data is limited to 365 Days. "
                                f"Only requesting for year '{from_dt.year}'")
            days = 365
            to_dt = datetime(from_dt.year, 12, 31, 23, 59, 59)
        if to_dt > datetime.today():
            to_dt = None

        contract = AsyncIBDataProvider.parse_contract(ticker)
        whatToShow = 'MIDPOINT' if isinstance(
            contract, (Forex, CFD, Commodity)) else 'TRADES'
        # bars = self.ib.reqDailyBars(contract, 2016)
        bars = self.ib.reqHistoricalData(contract, endDateTime=to_dt, durationStr=F"{days} D",
                                         barSizeSetting='1 day',
                                         whatToShow=whatToShow,
                                         useRTH=True,
                                         formatDate=1)
        return contract.symbol, bars

    def _to_dataframe(self, bars, tz_fix=False):
        if tz_fix:
            data = [{'Date': pd.to_datetime(b.date.astimezone(self.tz).replace(tzinfo=None)),
                     'Open': b.open, 'High': b.high, 'Low': b.low, 'Close': b.close,
                     'Volume': b.volume} for b in bars]
        else:
            data = [
                {'Date': pd.to_datetime(b.date),
                 'Open': b.open, 'High': b.high, 'Low': b.low, 'Close': b.close,
                 'Volume': b.volume} for b in bars]

        if len(data) > 0:
            return pd.DataFrame(data).set_index('Date')
        else:
            return pd.DataFrame()

    def add_quotes(self, data, ticker):
        return data


if __name__ == '__main__':
    ib = AsyncIBDataProvider()
    #df_list = ib.get_data(['JBL'], '2020-03-01', '2020-04-08', timeframe='60min', transform='60min')
    df_list = ib.get_dataframes(['JBL'], '2020-03-01',
                          '2020-04-08', timeframe='day', transform='day')

    for df in df_list:
        print(df.head())
        print(df.tail())
    # print(dailys[0].tail())
    # print(dailys[1].tail())
