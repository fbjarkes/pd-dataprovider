import unittest

import pandas_market_calendars as mcal

from qa_dataprovider.providers.csv_dataprovider import CsvFileDataProvider


class TestCsv(unittest.TestCase):

    def test_daily_trading_days(self):
        provider = CsvFileDataProvider(["data"])

        daily = provider.get_data(['DIS'],'2010-01-01','2017-01-01')[0]

        assert daily.loc['2016-01-04']['Day'] == 1 # Monday Jan. 4
        assert daily.loc['20160108']['Day'] == 5 # Friday Jan. 8
        assert daily.loc['20160111']['Day'] == 6  # Monday Jan. 11
        assert daily.loc['20161230']['Day'] == 252  #240? Friday Dec. 30

    def test_filter_days(self):
        provider = CsvFileDataProvider(["data"])

        nyse = mcal.get_calendar('NYSE')

        days = nyse.valid_days(start_date='2000-01-01', end_date='2017-01-01')
        daily = provider.get_data(['DIS'], '2000-01-01', '2017-01-01')[0]
        assert len(days) == len(daily)

    def test_infront_daily(self):
        provider = CsvFileDataProvider(["data"])
        nyse = mcal.get_calendar('NYSE')
        days = nyse.valid_days(start_date='2008-01-01', end_date='2015-12-31')
        daily_xlp = provider.get_data(['NYSF_XLP'], '2008-01-01', '2015-12-31')[0]

        assert str(daily_xlp.loc['2010-11-05']['High']) == '29.2301'

    def test_daily_trading_days(self):
        provider = CsvFileDataProvider(["data"])
        spy_daily = provider.get_data(['SPY'], '2010-01-01', '2017-01-01')[0]
        assert spy_daily.loc['20160104']['Day'] == 1  # Monday Jan. 4
        assert spy_daily.loc['20160108']['Day'] == 5  # Friday Jan. 8
        assert spy_daily.loc['20160111']['Day'] == 6  # Monday Jan. 11
        assert spy_daily.loc['20161230']['Day'] == 252  # 240? Friday Dec. 30

    def test_weekly_volume(self):
        provider = CsvFileDataProvider(["data"])
        spy_daily = provider.get_data(['SPY'], '2010-01-01', '2017-01-01')[0]
        spy_weekly = provider.get_data(['SPY'], '2010-01-01', '2017-01-01', timeframe='day',
                                       transform='week')[0]
        # print(spy_daily.loc['20160104'])
        # print(spy_daily.loc['20160105'])
        # print(spy_daily.loc['20160106'])
        # print(spy_daily.loc['20160107'])
        # print(spy_daily.loc['20160108'])
        # print(spy_weekly.loc['20160104'])

        assert spy_daily.loc['20160104':'20160108', 'Volume'].sum() == \
               spy_weekly.loc['20160104']['Volume']


if __name__ == '__main__':
    unittest.main()