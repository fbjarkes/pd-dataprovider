#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-


import sqlite3
import os.path
import pandas as pd
import numpy as np
import logging

#TODO: use "odo" to load into dataframe?
from qa_dataprovider.generic_dataprovider import GenericDataProvider


class SQLDataProvider(GenericDataProvider):

    TABLE = "TICKERS_5MIN"

    logging.basicConfig(level=logging.DEBUG, format='%(filename)s: %(message)s')
    logger = logging

    def add_quotes(self, data, ticker):
        return data

    def _get_data_internal(self, ticker: str, from_date: str, to_date: str, timeframe: str, transform: str) -> pd.DataFrame:
        conn = sqlite3.connect(self.dbPath)
        #limit = days * 79
        sql = f"select strftime('%Y-%m-%d %H:%M:%S', date, time) as datetime, open, high, low, close, vol as volume, rth from {self.TABLE} " \
              f"where symbol = '{ticker}' and date > '{from_date}' and date < '{to_date}' and rth = 1 order by date asc, time asc"
        df = pd.read_sql(sql, conn, parse_dates=['datetime'], index_col='datetime')

        #def _post_process(self, data, ticker, from_date, to_date, timeframe, transform, **kwargs):
        tmp = self._post_process(df, ticker, from_date, to_date, timeframe, transform)

        return tmp


    def __init__(self, dbPath):
        self.dbPath = dbPath

        if not os.path.isfile(dbPath):
            raise Exception("DB file does not exist: '%s'" % dbPath)

        # def fix_time(self, date, time):

    # d = timezone("US/Eastern").localize(parse(date + " " +time))
    #	return dt.date2num(d)

    # def get(self, symbol, startDate, endDate):
    #     conn = sqlite3.connect(self.dbPath)
    #     #limit = days * 79
    #     sql = "select strftime('%%Y-%%m-%%d %%H:%%M:%%S', date, time) as datetime, open, high, low, close, vol as volume, rth as openinterest from %s " \
    #           "where symbol = '%s' and date > '%s' and date < '%s' and rth = 1 order by date asc, time asc" % (self.TABLE, symbol, startDate, endDate)
    #     # sql = "select date, open, high, low, close, vol as volume, rth as openinterest from %s where symbol = '%s' order by date asc, time desc limit 500" % (self.TABLE, symbol)
    #     df = pd.read_sql(sql, conn, parse_dates=['datetime'], index_col='datetime')
    #     # print(df)
    #
    #     return df

        # def get(self, symbol):
        # 	conn = sqlite3.connect(self.dbPath)

        # 	cursor = conn.cursor()
        # 	#df = pd.DataFrame(np.random.randn(6,4),index=dates,columns=list('ABCD'))
        # 	datetimes = []
        # 	data = []
        # 	for row in cursor.execute("select * from %s where symbol = '%s' order by date asc, time desc limit 500" % (self.TABLE,symbol)):
        # 		s = row[1]
        # 		dt = self.fix_time(row[2],row[3])
        # 		o = float(row[4])
        # 		h = float(row[5])
        # 		l = float(row[6])
        # 		c = float(row[7])
        # 		vol = float(row[8])
        # 		oi = float(0)
        # 		data.append(tuple(o,h,l,c,vol,oi))
        # 		datetimes.append(dt)


        # 	df = pd.DataFrame(data, index=datetimes, columns=list())

        # 	return []


if __name__ == '__main__':
    data_provider = SQLDataProvider("/Users/fbjarkes/Dropbox/tickdata_db/tickdata_5min.db")
    data = data_provider.get_data(['SPY'], '2016-01-05 ', '2016-01-31', timeframe='5min', transform='5min')[0]
    print(data)