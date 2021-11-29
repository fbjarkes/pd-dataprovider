import unittest

from pd_dataprovider.objects import SymbolData, Data
from pd_dataprovider.providers.json_dataprovider import JSONDataProvider

class TestCsv(unittest.TestCase):

    def test_alpaca_intraday(self):
        provider = JSONDataProvider(['data/alpaca'], ['t', 'o', 'h', 'l', 'c', 'v'], epoch=True, verbose=2)
        symbol_datas = [SymbolData('SPY', '15min', '15min', '2020-05-11', '2020-05-13', rth_only=False),
                        SymbolData('SPY', '5min', '5min', '2020-05-11', '2020-05-13', rth_only=False)]

        datas = provider.get_datas(symbol_datas)
        assert datas[0].timeframe == '15min'
        assert datas[1].timeframe == '5min'
        assert datas[0].df.loc['2020-05-12 16:15']['Close'] == 285.48
        assert datas[1].df.loc['2020-05-12 16:35']['Close'] == 284.88


    def test_alpaca_week(self):
        provider = JSONDataProvider(['data/alpaca'],
                                    ['startEpochTime', 'openPrice', 'highPrice', 'lowPrice', 'closePrice', 'volume'],
                                    epoch=True, verbose=2)
        symbol_datas = [
            SymbolData('XONE', 'day', 'day', '2020-01-11', '2020-05-17', rth_only=True),
            SymbolData('XONE', 'day', 'week', '2020-01-11', '2020-05-17', rth_only=True)]

        datas = provider.get_datas(symbol_datas)
        daily = datas[0].df
        weekly = datas[1].df
        #assert d.timeframe == 'week'
        print(daily)
        print(weekly)



if __name__ == '__main__':
    unittest.main()