import sqlite3
import os.path
import pandas as pd
import numpy as np

#TODO: use "odo" to load into dataframe?

class SQLDataProvider:
    TABLE = "TICKERS_5MIN"

    def __init__(self, dbPath):
        self.dbPath = dbPath

        if not os.path.isfile(dbPath):
            raise Exception("DB file does not exist: '%s'" % dbPath)

        # def fix_time(self, date, time):

    # d = timezone("US/Eastern").localize(parse(date + " " +time))
    #	return dt.date2num(d)

    def get(self, symbol, startDate, endDate):
        conn = sqlite3.connect(self.dbPath)
        #limit = days * 79
        sql = "select strftime('%%Y-%%m-%%d %%H:%%M:%%S', date, time) as datetime, open, high, low, close, vol as volume, rth as openinterest from %s " \
              "where symbol = '%s' and date > '%s' and date < '%s' and rth = 1 order by date asc, time asc" % (self.TABLE, symbol, startDate, endDate)
        # sql = "select date, open, high, low, close, vol as volume, rth as openinterest from %s where symbol = '%s' order by date asc, time desc limit 500" % (self.TABLE, symbol)
        df = pd.read_sql(sql, conn, parse_dates=['datetime'], index_col='datetime')
        # print(df)

        return df

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
