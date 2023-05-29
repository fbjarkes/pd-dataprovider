import unittest

import pandas_market_calendars as mcal

from pd_dataprovider.objects import SymbolData
from pd_dataprovider.providers.csv_dataprovider import CsvFileDataProvider


class TestCsv(unittest.TestCase):

    def test_filter_days(self):
        provider = CsvFileDataProvider(["data"])
        nyse = mcal.get_calendar('NYSE')
        days = nyse.valid_days(start_date='2000-01-01', end_date='2017-01-01')
        daily = provider.get_dataframes([SymbolData('DIS', 'day', 'day', '2000-01-01', '2017-01-01')])[0]
        assert len(days) == len(daily)

    def test_infront_daily(self):
        provider = CsvFileDataProvider(["data"])
        nyse = mcal.get_calendar('NYSE')

        days = nyse.valid_days(start_date='2008-01-01', end_date='2015-12-31')
        daily_xlp = provider.get_dataframes([SymbolData('NYSF_XLP', 'day', 'day', '2008-01-01', '2015-12-31')])[0]
        assert str(daily_xlp.loc['2010-11-05']['High']) == '29.2301'

    def test_daily_trading_days(self):
        provider = CsvFileDataProvider(["data"])
        spy_daily = provider.get_dataframes([SymbolData('SPY', 'day', 'day', '2010-01-01', '2017-01-01')])[0]

        assert spy_daily.loc['20160104']['Day'] == 1  # Monday Jan. 4
        assert spy_daily.loc['20160108']['Day'] == 5  # Friday Jan. 8
        assert spy_daily.loc['20160111']['Day'] == 6  # Monday Jan. 11
        assert spy_daily.loc['20161230']['Day'] == 252  # 240? Friday Dec. 30

    def test_weekly_volume(self):
        provider = CsvFileDataProvider(["data"])

        spy_daily, spy_weekly = provider.get_dataframes([SymbolData('SPY', 'day', 'day', '2010-01-01', '2017-01-01'),
                                             SymbolData('SPY', 'day', 'week', '2010-01-01', '2017-01-01')])
        assert spy_daily.loc['20160104':'20160108', 'Volume'].sum() == \
               spy_weekly.loc['20160104']['Volume']


if __name__ == '__main__':
    unittest.main()