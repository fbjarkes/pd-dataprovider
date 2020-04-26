import asyncio
import sys
import traceback
import logging
from abc import ABCMeta, abstractmethod
import concurrent.futures
from functools import reduce
import pandas as pd

from qa_dataprovider.model.data import Data
from qa_dataprovider.utils.post_processor import PostProcessor
from qa_dataprovider.model.symbol_data import SymbolData
import qa_dataprovider.utils.log_helper as log_helper


class GenericDataProvider(metaclass=ABCMeta):

    post_processor = PostProcessor()
    chunk_size = 100

    logger = logging.getLogger(__name__)

    @abstractmethod
    def _get_data_internal(self, ticker: str, from_date: str, to_date: str, timeframe: str, transform: str) -> \
            pd.DataFrame:
        pass

    @abstractmethod
    def _get_data_internal_async(self, ticker: str, from_date: str, to_date: str, timeframe: str, transform: str) -> \
            pd.DataFrame:
        pass

    @abstractmethod
    def add_quotes(self, data, ticker):
        """
        If quotes are available by this provider, append quotes row to data.

        :param data dataframe: pd.DataFrame
        :param ticker string: The ticker, eg. 'SPY'
        :return: pd.DataFrame
        """
        pass

    def __init__(self, logger, verbose: int = 0, chunk_size: int = 100):
        self.errors = 0
        self.chunk_size = chunk_size
        log_helper.init_logging([self.logger, logger], verbose)

    def _initialize(self):
        """
        Do initialization, e.g. connecting etc.
        """
        pass

    def _finish(self):
        """        
        Do any post data fetching activities, e.g. disconnect, exit async loop, etc.
        """
        pass

    def get_datas(self, symbol_datas: [SymbolData]) -> [Data]:
        datas = []
        self._initialize()

        for symbol_data in symbol_datas:
            df = self._get_data_internal(symbol_data.symbol, symbol_data.start, symbol_data.end, symbol_data.timeframe, symbol_data.transform)
            #d = Data(df, symbol_data.symbol, symbol_data.timeframe, symbol_data.)
            datas.append(df)

        self.errors = len(datas) - len(symbol_data)
        self._finish()

        return datas

    def create_data_class(self, lst):
        datas = []
        for df, symbol_data in lst:
            if not df.empty:
                datas.append(Data(df, symbol_data.symbol, symbol_data.timeframe, df.index[0].to_pydatetime(), df.index[-1].to_pydatetime()))
        return datas

    def chunks(self, l, n):
        n = max(1, n)
        return (l[i:i + n] for i in range(0, len(l), n))

    async def get_datas_async(self, symbols_data: [SymbolData]) -> [Data]:
        self._initialize()
        chunks = self.chunks(symbols_data, self.chunk_size)
        datas = []
        for chunk in chunks:
            dfs = await asyncio.gather(*[self._get_data_internal_async(symbol_data) for symbol_data in chunk])
            datas += self.create_data_class(zip(dfs, chunk))
        self._finish()
        return datas

    #TODO: Use SymbolData
    def get_data(self, tickers, from_date, to_date, timeframe='day', transform='day',
                 max_workers=1, **kwargs) -> [pd.DataFrame]:
        """
        Fetch a dataframe for each ticker, using the internal method with multiple threads

        :param list tickers: A list of tickers
        :param string from_date: Start date, e.g. '2016-01-01'
        :param string to_date: End date, e.g. '2017-01-01'
        :param int workers: Number of threads
        :return: List with dataframes
        """
        dataframes = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._get_data_internal,
                                ticker, from_date, to_date, timeframe, transform, **kwargs): ticker
                for
                ticker in tickers}

            for future in concurrent.futures.as_completed(futures):
                ticker = futures[future]
                try:
                    data = future.result()
                except Exception as exc:
                    traceback.print_exc(file=sys.stderr)
                    logger.debug("", exc_info=True)
                    logger.warning(
                        "Skipping {}: error message: {}".format(ticker, exc))
                else:
                    if data is not None:
                        dataframes.append(data)

        self.errors = len(dataframes) - len(tickers)
        self._finish()

        return dataframes

    def _post_process(self, data, ticker, from_date, to_date, timeframe, transform, **kwargs):
        func_args = {
            'ticker': ticker,
            'timeframe': timeframe,
            'transform': transform,
            'from': from_date,
            'to': to_date,
            'provider': self,
        }
        func_args.update(**kwargs)
        # Post process data in this order:
        funcs = [self.post_processor.filter_dates,
                 self.post_processor.filter_rth,
                 self.post_processor.validate,
                 self.post_processor.add_quotes,
                 self.post_processor.transform_timeframe,
                 self.post_processor.fill_na,
                 self.post_processor.add_trading_days,
                 self.post_processor.add_meta_data,
                 #self.post_processor.make_data_class
                 ]

        return reduce((lambda result, func: func(result, func_args)), funcs, data)
