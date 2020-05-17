import logging
import os
import json
import pandas as pd

from qa_dataprovider.providers.generic_dataprovider import GenericDataProvider
from qa_dataprovider.objects import SymbolData


class JSONDataProvider(GenericDataProvider):

    logging.basicConfig(level=logging.DEBUG, format='%(filename)s: %(message)s')
    logger = logging.getLogger(__name__)

    def __init__(self, paths, keys, verbose = 0, epoch=False):
        super(JSONDataProvider, self).__init__(self.logger, verbose, tz='America/New_York')
        self.paths = paths
        self.keys = keys
        self.epoch = epoch

    async def _get_data_internal_async(self, symbol_data: SymbolData, **kwargs) -> pd.DataFrame:
        return self._get_data_internal(symbol_data, **kwargs)

    def _get_data_internal(self, symbol_data: SymbolData, **kwargs) -> pd.DataFrame:
        for path in self.paths:
            filename = (f"{path}/{symbol_data.timeframe}/{symbol_data.symbol}.json")
            self.logger.debug(f"Trying '{filename}'")
            if os.path.exists(filename):
                with open(filename) as f:
                    jsonData =json.load(f)
                    if jsonData[symbol_data.symbol]:
                        df = pd.DataFrame(jsonData[symbol_data.symbol], columns=self.keys)
                        df.rename(columns={self.keys[0]: GenericDataProvider.DEFAULT_COL_NAMES[0],
                                           self.keys[1]: GenericDataProvider.DEFAULT_COL_NAMES[1],
                                           self.keys[2]: GenericDataProvider.DEFAULT_COL_NAMES[2],
                                           self.keys[3]: GenericDataProvider.DEFAULT_COL_NAMES[3],
                                           self.keys[4]: GenericDataProvider.DEFAULT_COL_NAMES[4],
                                           self.keys[5]: GenericDataProvider.DEFAULT_COL_NAMES[5]},
                                  inplace = True)
                        df.set_index(GenericDataProvider.DEFAULT_COL_NAMES[0], inplace=True)
                        df.index = pd.to_datetime(df.index, unit='s', utc=True).tz_convert(self.tz).tz_localize(None)
                        self.logger.info("{}, {:d} rows ({} to {})" .format(f.name, len(df), df.index[0], df.index[-1]))

                        data = self._post_process(df, symbol_data.symbol, symbol_data.start, symbol_data.end,
                                                  symbol_data.timeframe, symbol_data.transform,
                                                  rth_only=symbol_data.rth_only, **kwargs)
                        return data

        if 'graceful' in kwargs and kwargs['graceful']:
            self.logger.warning("{} not found in {}".format(symbol_data.symbol, self.paths))
            return pd.DataFrame()
        else:
            raise Exception("{} not found in {}".format(symbol_data.symbol, self.paths))
