import asyncio
import logging
import os

from pd_dataprovider.providers.generic_dataprovider import GenericDataProvider
import pandas as pd
import numpy as np
from pd_dataprovider.objects import SymbolData


class CsvFileDataProvider(GenericDataProvider):
    """
    Expecting data to have Columns (case sensitive): Date, Open, High, Low, Close, Volume
    Example:
        Date,Open,High,Low,Close,Change,Settle,Volume,Previous Day Open Interest
        2017-11-02,54.53,55.1,54.27,55.04,0.29,54.89,35326.0,95525.0
        ...
    """
    DEFAULT_COL_NAMES = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    logging.basicConfig(level=logging.DEBUG,
                        format='%(filename)s: %(message)s')
    logger = logging.getLogger(__name__)

    def __init__(self, in_paths, verbose=0, prefix=None, col_names=None, epoch=False, **kwargs):
        """
        Initialize with a list of paths for which each call to get_data() tries to open
        csv file directly in paths. 

        For example calling get_data() with ticker 'SPY' using paths ['data/stocks', 'data/etf'] 
        will try to open 'data/stocks/SPY.csv' and 'data/etf/SPY.csv' with first found file 
        being used.

        :param list in_paths: A list of paths containing csv-files
        :param list prefix: An optional list of prefixes e.g. ['NYS','NYSF'] will find file  
        "NYS_AAPL.csv" or "NYSF_AAPL.csv"
        :param list col_names: Specify custom column names
        :param epoch: Datetimes in epoch or as string
        """
        super(CsvFileDataProvider, self).__init__(self.logger, verbose, tz='America/New_York', **kwargs)
        if prefix is None:
            prefix = []
        if col_names is None:
            col_names = CsvFileDataProvider.DEFAULT_COL_NAMES

        self.paths = in_paths
        self.prefix = prefix
        self.col_names = col_names
        self.epoch = epoch

    @DeprecationWarning
    async def _get_data_internal_async(self, symbol_data: SymbolData, **kwargs) -> pd.DataFrame:
        for path in self.paths:
            # filenames = [
            #     "{}/{}_{}.{}".format(path, prefix, symbol_data.symbol, 'csv') for prefix in self.prefix]
            # filenames.append("{}/{}.{}".format(path, symbol_data.symbol, 'csv'))
            filename = (f"{path}/{symbol_data.timeframe}/{symbol_data.symbol}.csv")
            self.logger.debug("Trying '{}'".format(filename))
            if os.path.exists(filename):
                with open(filename) as f:
                    # async with AIOFile(filename, 'r') as f:
                    # TODO: check if file contains any data? To avoid ambigous async error if empty file
                    df = pd.read_csv(f, dtype={self.col_names[1]: np.float32, self.col_names[2]: np.float32,
                                               self.col_names[3]: np.float32,
                                               self.col_names[4]: np.float32, self.col_names[5]: np.float32},
                                     parse_dates=True, index_col=self.col_names[0])
                    df = df.sort_index()
                    if self.epoch:
                        df.index = pd.to_datetime(df.index, unit='s')

                    if not all(elem in self.col_names for elem in self.DEFAULT_COL_NAMES):
                        df.rename(columns={self.col_names[1]: 'Open', self.col_names[2]: 'High',
                                           self.col_names[3]: 'Low', self.col_names[4]: 'Close',
                                           self.col_names[5]: 'Volume'},
                                  inplace=True)
                    self.logger.info("{}, {:d} rows ({} to {})"
                                     .format(f.name, len(df), df.index[0], df.index[-1]))

                    data = self._post_process(df, symbol_data.symbol, symbol_data.start, symbol_data.end,
                                              symbol_data.timeframe, symbol_data.transform,
                                              rth_only=symbol_data.rth_only, **kwargs)
                    return data
        if 'graceful' in kwargs and kwargs['graceful']:
            self.logger.warning(f"Could not find or open {symbol_data.symbol} in {self.paths}")
            return pd.DataFrame()
        else:
            raise Exception("{} not found in {}".format(symbol_data.symbol, self.paths))

    def _get_data_internal(self, symbol_data: SymbolData, **kwargs) -> pd.DataFrame:
        drop_non_default_columns = kwargs.get('drop_non_default_columns', False)
        for path in self.paths:
            filenames = [
                "{}/{}/{}_{}.{}".format(path, symbol_data.timeframe, prefix, symbol_data.symbol, 'csv') for prefix in
                self.prefix]
            filenames.append("{}/{}/{}.{}".format(path, symbol_data.timeframe, symbol_data.symbol, 'csv'))
            for filename in filenames:
                self.logger.debug("Trying '{}'".format(filename))
                if os.path.exists(filename):
                    with open(filename) as f:
                        try:
                            df = pd.read_csv(f, dtype={self.col_names[1]: np.float32, self.col_names[2]: np.float32,
                                                       self.col_names[3]: np.float32,
                                                       self.col_names[4]: np.float32, self.col_names[5]: np.float32},
                                             parse_dates=True, index_col=self.col_names[0])
                            df = df.sort_index()
                            if self.epoch:
                                # df.index = pd.to_datetime(df.index, unit='s')
                                df.index = pd.to_datetime(df.index, unit='s', utc=True).tz_convert(self.tz).tz_localize(
                                    None)

                            if not all(elem in self.col_names for elem in self.DEFAULT_COL_NAMES):
                                df.rename(columns={self.col_names[1]: 'Open', self.col_names[2]: 'High',
                                                   self.col_names[3]: 'Low', self.col_names[4]: 'Close',
                                                   self.col_names[5]: 'Volume'},
                                          inplace=True)

                            if drop_non_default_columns:
                                df.drop(columns=[col for col in df if col not in self.DEFAULT_COL_NAMES], inplace=True)

                            self.logger.info("{}, {:d} rows ({} to {})"
                                             .format(f.name, len(df), df.index[0], df.index[-1]))

                            data = self._post_process(df, symbol_data.symbol, symbol_data.start, symbol_data.end,
                                                      symbol_data.timeframe, symbol_data.transform,
                                                      rth_only=symbol_data.rth_only, **kwargs)
                            return data
                        except Exception as e:
                            self.logger.warning(f"Error reading and processsing df: ", e)

        if 'graceful' in kwargs and kwargs['graceful']:
            self.logger.warning(f"Could not find or open {symbol_data.symbol} in {self.paths}")
            return pd.DataFrame()
        else:
            raise Exception("{} not found in {}".format(symbol_data.symbol, self.paths))

    def add_quotes(self, data, ticker):
        """
        Quotes are not available...         
        """
        return data


if __name__ == '__main__':
    paths = [f"/Users/{os.environ['USER']}/OneDrive/Data/tradingview"]
    tradingview = CsvFileDataProvider(paths, col_names=['time', 'open', 'high', 'low', 'close', 'Volume'], epoch=True)
    # alphavantage = CsvFileDataProvider(paths, col_names=['timestamp', 'open', 'high', 'low', 'close', 'Volume'], epoch=False)
    # data = provider.get_data(['OMXS30 F18-OMF_1 Minute'], '2017-12-01', '2017-12-31', timeframe='1min', transform='1h')
    symbols_data = [
        SymbolData('BIDU', '5min', '5min', '2021-02-24 09:30', '2021-02-26 16:00')
        # SymbolData('SKA_B_1H', '60min', '60min', '2020-04-02 10:00:00', '2020-04-03 15:00:00')
    ]
    datas = asyncio.run(tradingview.get_datas_async(symbols_data))
    df = datas[0].df
    print(df.tail())
