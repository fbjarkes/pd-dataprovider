import asyncio
import logging
import os

from qa_dataprovider.providers.generic_dataprovider import GenericDataProvider
import pandas as pd
import numpy as np
from qa_dataprovider.objects import SymbolData


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

    def __init__(self, paths, verbose = 0, prefix=[], col_names=DEFAULT_COL_NAMES, epoch=False):
        """
        Initialize with a list of paths for which each call to get_data() tries to open
        csv file directly in paths. 

        For example calling get_data() with ticker 'SPY' using paths ['data/stocks', 'data/etf'] 
        will try to open 'data/stocks/SPY.csv' and 'data/etf/SPY.csv' with first found file 
        being used.

        :param list paths: A list of paths containing csv-files  
        :param list prefix: An optional list of prefixes e.g. ['NYS','NYSF'] will find file  
        "NYS_AAPL.csv" or "NYSF_AAPL.csv"
        :param list col_names: Specify custom column names
        :param epoch: Datetimes in epoch or as string
        """
        super(CsvFileDataProvider, self).__init__(self.logger, verbose, tz='America/New_York')
        self.paths = paths
        self.prefix = prefix
        self.col_names = col_names
        self.epoch = epoch

    async def _get_data_internal_async(self, symbol_data: SymbolData, **kwargs) -> pd.DataFrame:
        for path in self.paths:
            filenames = [
                "{}/{}_{}.{}".format(path, prefix, symbol_data.symbol, 'csv') for prefix in self.prefix]
            filenames.append("{}/{}.{}".format(path, symbol_data.symbol, 'csv'))

            for filename in filenames:
                self.logger.debug("Trying '{}'".format(filename))
                if os.path.exists(filename):
                    with open(filename) as f:
                    #async with AIOFile(filename, 'r') as f:
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

                        data = self._post_process(df, symbol_data.symbol, symbol_data.start, symbol_data.end, symbol_data.timeframe, symbol_data.transform, **kwargs)
                        return data
        raise Exception("{} not found in {}".format(symbol_data.symbol, self.paths))

    def _get_data_internal(self, symbol_data: SymbolData, **kwargs) -> pd.DataFrame:
        for path in self.paths:
            filenames = [
                "{}/{}_{}.{}".format(path, prefix, symbol_data.symbol, 'csv') for prefix in self.prefix]
            filenames.append("{}/{}.{}".format(path, symbol_data.symbol, 'csv'))

            for filename in filenames:
                self.logger.debug("Trying '{}'".format(filename))
                if os.path.exists(filename):
                    with open(filename) as f:
                        df = pd.read_csv(f, dtype={self.col_names[1]: np.float32, self.col_names[2]: np.float32, self.col_names[3]: np.float32,
                                                   self.col_names[4]: np.float32, self.col_names[5]: np.float32}, parse_dates=True, index_col=self.col_names[0])
                        df = df.sort_index()
                        if self.epoch:
                            df.index = pd.to_datetime(df.index, unit='s')

                        if not all(elem in self.col_names for elem in self.DEFAULT_COL_NAMES):
                            df.rename(columns = {self.col_names[1]: 'Open', self.col_names[2]: 'High',
                                                 self.col_names[3]: 'Low', self.col_names[4]: 'Close',
                                                 self.col_names[5]: 'Volume'},
                                      inplace = True)
                        self.logger.info("{}, {:d} rows ({} to {})"
                                         .format(f.name, len(df), df.index[0], df.index[-1]))

                        data = self._post_process(df, symbol_data.symbol, symbol_data.start, symbol_data.end, symbol_data.timeframe, symbol_data.transform, **kwargs)
                        return data

        self.logger.info("{} not found in {}".format(symbol_data.symbol, self.paths))

    def add_quotes(self, data, ticker):
        """
        Quotes are not available...         
        """
        return data



if __name__ == '__main__':
    paths = [f"/Users/{os.environ['USER']}/Dropbox/csv/"]
    tradingview = CsvFileDataProvider(paths, col_names=['time','open','high','low','close','Volume'], epoch=True)
    #alphavantage = CsvFileDataProvider(paths, col_names=['timestamp', 'open', 'high', 'low', 'close', 'Volume'], epoch=False)
    #data = provider.get_data(['OMXS30 F18-OMF_1 Minute'], '2017-12-01', '2017-12-31', timeframe='1min', transform='1h')
    symbols_data = [
        #{'symbol': 'SKA_B_1H', 'timeframe': '60min', 'transform': '60min'},
        SymbolData('SKA_B_1H', '60min', '60min', '2020-04-02 10:00:00', '2020-04-03 15:00:00')
    ]
    datas = asyncio.run(tradingview.get_datas_async(symbols_data))
    print(datas)

