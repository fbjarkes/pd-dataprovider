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

    def test_alpaca_intraday_v2(self):
        provider = JSONDataProvider(['data/alpaca-v2'],
                                    ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume'],
                                    verbose=2)

    def test_daily_with_snapshots(self):
        provider = JSONDataProvider(['data/alpaca-v2'],
                                    ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume'],
                                    verbose=2)
        sym_datas = [SymbolData('AAPL', 'day', 'day', '2021-01-01', '2021-12-02')]
        df = provider.get_datas(sym_datas, snapshots=True)[0].df
        assert df.loc['2021-01-05']['Close'] == 130.20761
        assert df.loc['2021-11-30']['Close'] == 165.3   # Last bar from daily file
        assert df.loc['2021-12-01']['Close'] == 164.77  # Single bar from snapshots file



if __name__ == '__main__':
    unittest.main()