#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-


import sqlite3
import os.path
import pandas as pd
import logging

#TODO: use "odo" to load into dataframe?
from qa_dataprovider.providers.generic_dataprovider import GenericDataProvider


class SQLDataProvider(GenericDataProvider):

    TABLE = "TICKERS_5MIN"

    logging.basicConfig(level=logging.DEBUG, format='%(filename)s: %(message)s')
    logger = logging

    def add_quotes(self, data, ticker):
        return data

    def _get_data_internal(self, ticker: str, from_date: str, to_date: str, timeframe: str,
                           transform: str, **kwargs) -> pd.DataFrame:
        conn = sqlite3.connect(self.dbPath)
        #limit = days * 79
        sql = f"select strftime('%Y-%m-%d %H:%M:%S', date, time) as datetime, open, high, low, close, vol as volume, rth from {self.TABLE} " \
              f"where symbol = '{ticker}' and date > '{from_date}' and date < '{to_date}' and rth = 1 order by date asc, time asc"
        df = pd.read_sql(sql, conn, parse_dates=['datetime'], index_col='datetime')

        #def _post_process(self, data, ticker, from_date, to_date, timeframe, transform, **kwargs):
        tmp = self._post_process(df, ticker, from_date, to_date, timeframe, transform)
        return tmp

    def __init__(self, paths):
        self.dbPath = None

        for path in paths:
            if os.path.isfile(path):
                self.dbPath = path

        if not self.dbPath:
            raise Exception(f"DB file does not exist: {paths}")

if __name__ == '__main__':
    data_provider = SQLDataProvider(["/home/fbjarkes/Dropbox/Tickdata_db/tickdata_5min.db"])
    data = data_provider.get_dataframes(['SPY'], '2016-01-05 ', '2016-01-31', timeframe='5min', transform='5min')[0]
    print(data)