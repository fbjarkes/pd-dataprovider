import asyncio
import sys
import traceback
import logging
from abc import ABCMeta, abstractmethod
import concurrent.futures
from functools import reduce
import pandas as pd
import pytz

from pd_dataprovider.utils.post_processor import PostProcessor
from pd_dataprovider.objects import SymbolData, Data
import pd_dataprovider.utils.log_helper as log_helper


class GenericDataProvider(metaclass=ABCMeta):

    chunk_size = 100

    DEFAULT_COL_NAMES = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']

    _logger = logging.getLogger(__name__)

    @abstractmethod
    def _get_data_internal(self, symbol_data: SymbolData, **kwargs) -> \
            pd.DataFrame:
        pass

    @abstractmethod
    def _get_data_internal_async(self, ticker: str, from_date: str, to_date: str, timeframe: str, transform: str,
                                 **kwargs) -> pd.DataFrame:
        pass

    def __init__(self, logger, verbose: int, tz, chunk_size: int = 6, **kwargs):
        self.errors = 0
        self.chunk_size = chunk_size
        self.tz = pytz.timezone(tz)
        log_helper.init_logging([self._logger, logger], verbose)
        self.post_processor = PostProcessor(logger, **kwargs)

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

    def get_datas(self, symbol_datas: [SymbolData], **kwargs) -> [Data]:
        datas = []
        self._initialize()
        chunks = self.chunks(symbol_datas, self.chunk_size)
        for chunk in chunks:
            dataframes = []
            for symbol_data in chunk:
                df = self._get_data_internal(symbol_data, **kwargs)
                dataframes.append(df)
            datas += self.create_data_class(zip(dataframes, chunk))

        self.errors = len(datas) - len(symbol_datas)
        self._finish()

        return datas

    def create_data_class(self, lst):
        datas = []
        for df, symbol_data in lst:
            if df.symbol != symbol_data.symbol:
                raise Exception(f"Symbol data missmats: {df.symbol} != {symbol_data.symbol}")
            if not df.empty:
                datas.append(Data(df, symbol_data.symbol, symbol_data.transform,
                                  df.index[0].to_pydatetime(), df.index[-1].to_pydatetime()))
        return datas

    def chunks(self, l, n):
        n = max(1, n)
        return (l[i:i + n] for i in range(0, len(l), n))

    async def get_datas_async(self, symbol_datas: [SymbolData], **kwargs) -> [Data]:
        self._initialize()
        chunks = self.chunks(symbol_datas, self.chunk_size)
        datas = []
        for chunk in chunks:
            funcs = [self._get_data_internal_async(
                symbol_data, **kwargs) for symbol_data in chunk]
            dfs = await asyncio.gather(*funcs)
            datas += self.create_data_class(zip(dfs, chunk))
        self._finish()
        return datas

    def get_dataframes(self, symbol_datas: [SymbolData]) -> [pd.DataFrame]:
        dataframes = []
        self._initialize()
        for symbol_data in symbol_datas:
            try:
                dataframes.append(self._get_data_internal(symbol_data))
            except Exception as exc:
                traceback.print_exc(file=sys.stderr)
                self.logger.warning(
                    "Skipping {}: error message: {}".format(symbol_data.symbol, exc))

        self.errors = len(dataframes) - len(symbol_datas)
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
                 self.post_processor.transform_timeframe,
                 self.post_processor.fill_na,
                 self.post_processor.add_meta_data,
                 ]

        return reduce((lambda result, func: func(result, func_args)), funcs, data)
