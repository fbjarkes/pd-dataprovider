import logging
import os
import json

import aiofiles
import pandas as pd

from pd_dataprovider.providers.generic_dataprovider import GenericDataProvider
from pd_dataprovider.objects import SymbolData


class JSONDataProvider(GenericDataProvider):

    logging.basicConfig(level=logging.DEBUG,
                        format='%(filename)s: %(message)s')
    logger = logging.getLogger(__name__)

    def __init__(self, paths, keys, verbose=0, epoch=False, **kwargs):
        super(JSONDataProvider, self).__init__(
            self.logger, verbose, tz='America/New_York', **kwargs)
        self.paths = paths
        self.keys = keys
        self.epoch = epoch

    async def _get_data_internal_async(self, symbol_data: SymbolData, **kwargs) -> pd.DataFrame:
        for path in self.paths:
            filename = (
                f"{path}/{symbol_data.timeframe}/{symbol_data.symbol}.json")
            self.logger.debug(f"Trying '{filename}'")
            if os.path.exists(filename):
                async with aiofiles.open(filename, mode='r') as f:
                    contents = await f.read()
                    jsonData = json.loads(contents)
                    if jsonData[symbol_data.symbol]:
                        df = pd.DataFrame(
                            jsonData[symbol_data.symbol], columns=self.keys)
                        df.rename(columns={self.keys[0]: GenericDataProvider.DEFAULT_COL_NAMES[0],
                                           self.keys[1]: GenericDataProvider.DEFAULT_COL_NAMES[1],
                                           self.keys[2]: GenericDataProvider.DEFAULT_COL_NAMES[2],
                                           self.keys[3]: GenericDataProvider.DEFAULT_COL_NAMES[3],
                                           self.keys[4]: GenericDataProvider.DEFAULT_COL_NAMES[4],
                                           self.keys[5]: GenericDataProvider.DEFAULT_COL_NAMES[5]},
                                  inplace=True)
                        df.set_index(
                            GenericDataProvider.DEFAULT_COL_NAMES[0], inplace=True)
                        df.index = pd.to_datetime(df.index, unit='s', utc=True).tz_convert(
                            self.tz).tz_localize(None)
                        self.logger.info(
                            f"{filename}: {len(df)} ({df.index[0]} - {df.index[-1]})")
                        data = self._post_process(df, symbol_data.symbol, symbol_data.start, symbol_data.end,
                                                  symbol_data.timeframe, symbol_data.transform,
                                                  rth_only=symbol_data.rth_only, **kwargs)
                        data.symbol = symbol_data.symbol
                        return data
        if 'graceful' in kwargs and kwargs['graceful']:
            self.logger.warning("{} not found in {}".format(
                symbol_data.symbol, self.paths))
            df = pd.DataFrame()
            df.symbol = symbol_data.symbol
            return df
        else:
            raise Exception("{} not found in {}".format(
                symbol_data.symbol, self.paths))

    def json_to_df(self, filename: str, symbol_data: SymbolData) -> pd.DataFrame:
        with open(filename) as f:
            json_data = json.load(f)
            if json_data[symbol_data.symbol]:
                df = pd.DataFrame(
                    json_data[symbol_data.symbol], columns=self.keys)
                df.rename(columns={self.keys[0]: GenericDataProvider.DEFAULT_COL_NAMES[0],
                                   self.keys[1]: GenericDataProvider.DEFAULT_COL_NAMES[1],
                                   self.keys[2]: GenericDataProvider.DEFAULT_COL_NAMES[2],
                                   self.keys[3]: GenericDataProvider.DEFAULT_COL_NAMES[3],
                                   self.keys[4]: GenericDataProvider.DEFAULT_COL_NAMES[4],
                                   self.keys[5]: GenericDataProvider.DEFAULT_COL_NAMES[5]},
                          inplace=True)
                df.set_index(
                    GenericDataProvider.DEFAULT_COL_NAMES[0], inplace=True)
                if self.epoch:
                    df.index = pd.to_datetime(df.index, unit='s', utc=True).tz_convert(
                        self.tz).tz_localize(None)
                else:
                    df.index = pd.to_datetime(df.index, utc=True).tz_convert(
                        self.tz).tz_localize(None)
                self.logger.info("{}, {:d} rows ({} to {})".format(
                    f.name, len(df), df.index[0], df.index[-1]))
                return df
            else:
                self.logger.warning(
                    f"Could not find '{symbol_data.symbol}' in file '{filename}'")
            return pd.DataFrame()

    def _get_data_internal(self, symbol_data: SymbolData, **kwargs) -> pd.DataFrame:
        for path in self.paths:
            filename = f"{path}/{symbol_data.timeframe}/{symbol_data.symbol}.json"
            self.logger.debug(f"Trying '{filename}'")
            if os.path.exists(filename):
                df = self.json_to_df(filename, symbol_data)
                df = self.append_snapshots(df, path, symbol_data, kwargs)
                data = self._post_process(df, symbol_data.symbol, symbol_data.start, symbol_data.end,
                                          symbol_data.timeframe, symbol_data.transform,
                                          rth_only=symbol_data.rth_only, **kwargs)
                data.symbol = symbol_data.symbol
                return data

        if 'graceful' in kwargs and kwargs['graceful']:
            self.logger.warning("{} not found in {}".format(
                symbol_data.symbol, self.paths))
            df = pd.DataFrame()
            df.symbol = symbol_data.symbol
            return df
        else:
            raise Exception("{} not found in {}".format(
                symbol_data.symbol, self.paths))

    def append_snapshots(self, df: pd.DataFrame, path: str, symbol_data: SymbolData, kwargs: dict) -> pd.DataFrame:
        if not df.empty and 'snapshots' in kwargs and kwargs['snapshots'] and symbol_data.timeframe == 'day':
            snapshot_filename = f"{path}/snapshots/{symbol_data.symbol}.json"
            self.logger.debug(
                f"Trying snapshot file: '{snapshot_filename}'")
            snapshot_df = self.json_to_df(
                snapshot_filename, symbol_data)
            if snapshot_df.empty:
                self.logger.warning(
                    f"Failed to load snapshot file '{snapshot_filename}'")
            else:
                if snapshot_df.index.isin(df.index):
                    self.logger.debug(
                        f"Not adding already existing data point for snapshot '{snapshot_filename}'")
                else:
                    df = df.append(snapshot_df)
        return df
