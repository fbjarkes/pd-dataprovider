#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import unittest

from qa_dataprovider.objects import SymbolData
from qa_dataprovider.provider_factory import ProviderFactory
from qa_dataprovider.providers.csv_dataprovider import CsvFileDataProvider
import pandas as pd


class TestPostProcessor(unittest.TestCase):


    def test_filter_rth(self):
        provider = CsvFileDataProvider(["data"])
        df = provider.get_dataframes([SymbolData('AAPL_2017-11-03', '5min', '5min', '2017-10-10', '2017-10-31', rth_only=True)])[0]
        assert len(df.at_time('09:25:00')) == 0
        assert len(df.at_time('09:30:00')) > 0
        assert len(df.at_time('16:05:00')) == 0

    def test_resample_timeframe(self):
        provider = CsvFileDataProvider(["data"], verbose=2)
        #df = provider.get_data(['AAPL_2018-01-06'], '2017-12-10', '2017-12-31', timeframe='5min', transform='60min', rth=True)[0]
        df = provider.get_dataframes([SymbolData('AAPL_2018-01-06', '5min', '60min', '2017-12-10', '2017-12-31', rth_only=True)])[0]

        # Assert 2017-12-29 09:30: O=170.71, H=170.71, L=169.71, C=169.93
        assert str(df.loc[pd.to_datetime('2017-12-29 09:00:00')]['Open']) == "170.71"
        assert str(df.loc[pd.to_datetime('2017-12-29 09:00:00')]['High']) == "170.71"
        assert str(df.loc[pd.to_datetime('2017-12-29 09:00:00')]['Low']) == "169.71"
        assert str(df.loc[pd.to_datetime('2017-12-29 09:00:00')]['Close']) == "169.93"

        # Assert 2017-12-29 10:00: O=169.92, H=169.95, L=169.31, C=169.64
        assert str(df.loc[pd.to_datetime('2017-12-29 10:00:00')]['Open']) == "169.92"
        assert str(df.loc[pd.to_datetime('2017-12-29 10:00:00')]['High']) == "169.95"
        assert str(df.loc[pd.to_datetime('2017-12-29 10:00:00')]['Low']) == "169.31"
        assert str(df.loc[pd.to_datetime('2017-12-29 10:00:00')]['Close']) == "169.64"

        # Assert 2017-12-29 16:00: O=169.23
        assert str(df.loc[pd.to_datetime('2017-12-29 16:00:00')]['Open']) == "169.23"


    def test_resample_multi_timeframes(self):
        provider = CsvFileDataProvider(["data"], verbose=2)
        (min60, daily, weekly) = provider.get_datas([SymbolData('VOLV.B_5min', '5min', '60min','','', rth_only=False),
                                                     SymbolData('VOLV.B_5min', '5min', 'day','','', rth_only=False),
                                                     SymbolData('VOLV.B_5min', '5min', 'week','','', rth_only=False)])

        assert str(min60.df.loc[pd.to_datetime('2020-04-01 10:00:00')]['Open']) == "115.45"
        assert str(daily.df.loc[pd.to_datetime('2020-04-01')]['Open']) == "116.0"
        assert str(round(daily.df.loc[pd.to_datetime('2020-04-01')]['Close'],2)) == "112.83"
        assert str(weekly.df.loc[pd.to_datetime('2020-04-14')]['Open']) == "130.0"



    def test_resample_timeframe_metadata(self):
        provider = CsvFileDataProvider(["data"], verbose=2)
        datas = provider.get_datas([SymbolData('SPY', 'day', 'day', '2016-01-01', '2016-12-31'),
                                    SymbolData('SPY', 'day', 'week', '2016-01-01', '2016-12-31'),
                                    SymbolData('SPY', 'day', 'month', '2016-01-01', '2016-12-31')])

        assert datas[0].timeframe == 'day'
        assert datas[1].timeframe == 'week'
        assert datas[2].timeframe == 'month'


    # def test_vwap(self):
    #     provider = CsvFileDataProvider(
    #             ['data'],
    #             verbose=2,
    #             col_names=['time','open','high','low','close', 'VWAP', 'MA', 'MA Volume'],
    #             epoch=True)
    #     datas = provider.get_datas([SymbolData('SPY_VWAP', '5min', '5min', '', '', 1)])
    #     df = datas[0].df
    #     curr_day = 26
    #     sum = 0
    #     for index, row in df.iterrows():
    #         if index.day != curr_day:
    #             print('VWAP for session day', curr_day, sum)
    #             sum = row[4]
    #             curr_day = index.day
    #         else:
    #             sum += row[4]
    #         #print(index, row[4])

    def test_cons_highs_lows(self):
        provider = CsvFileDataProvider(["data"], verbose=2, ta={'linearity': True})
        datas = provider.get_datas([SymbolData('SPY', 'day', 'day', '2016-01-01', '2016-01-15')])
        df = datas[0].df

        assert df.loc['2016-01-04']['ConsOpen'] == 0
        assert df.loc['2016-01-05']['ConsOpen'] == 1
        assert df.loc['2016-01-06']['ConsOpen'] == -1
        assert df.loc['2016-01-07']['ConsOpen'] == -2
        assert df.loc['2016-01-08']['ConsOpen'] == -3
        assert df.loc['2016-01-11']['ConsOpen'] == -4
        assert df.loc['2016-01-12']['ConsOpen'] == 1
        assert df.loc['2016-01-13']['ConsOpen'] == 2
        assert df.loc['2016-01-14']['ConsOpen'] == -1
        assert df.loc['2016-01-15']['ConsOpen'] == -2

        assert df.loc['2016-01-04']['ConsHigh'] == 0
        assert df.loc['2016-01-05']['ConsHigh'] == 1
        assert df.loc['2016-01-06']['ConsHigh'] == -1
        assert df.loc['2016-01-07']['ConsHigh'] == -2
        assert df.loc['2016-01-08']['ConsHigh'] == -3
        assert df.loc['2016-01-11']['ConsHigh'] == -4
        assert df.loc['2016-01-12']['ConsHigh'] == 1
        assert df.loc['2016-01-13']['ConsHigh'] == 2
        assert df.loc['2016-01-14']['ConsHigh'] == -1
        assert df.loc['2016-01-15']['ConsHigh'] == -2

        assert df.loc['2016-01-04']['ConsLow'] == 0
        assert df.loc['2016-01-05']['ConsLow'] == 1
        assert df.loc['2016-01-06']['ConsLow'] == -1
        assert df.loc['2016-01-07']['ConsLow'] == -2
        assert df.loc['2016-01-08']['ConsLow'] == -3
        assert df.loc['2016-01-11']['ConsLow'] == -4
        assert df.loc['2016-01-12']['ConsLow'] == 1
        assert df.loc['2016-01-13']['ConsLow'] == -1
        assert df.loc['2016-01-14']['ConsLow'] == -2
        assert df.loc['2016-01-15']['ConsLow'] == -3

        assert df.loc['2016-01-04']['ConsClose'] == 0
        assert df.loc['2016-01-05']['ConsClose'] == 1
        assert df.loc['2016-01-06']['ConsClose'] == -1
        assert df.loc['2016-01-07']['ConsClose'] == -2
        assert df.loc['2016-01-08']['ConsClose'] == -3
        assert df.loc['2016-01-11']['ConsClose'] == 1
        assert df.loc['2016-01-12']['ConsClose'] == 2
        assert df.loc['2016-01-13']['ConsClose'] == -1
        assert df.loc['2016-01-14']['ConsClose'] == 1
        assert df.loc['2016-01-15']['ConsClose'] == -1




if __name__ == '__main__':
    unittest.main()