import unittest

from qa_dataprovider.objects import SymbolData, Data
from qa_dataprovider.providers.json_dataprovider import JSONDataProvider

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






if __name__ == '__main__':
    unittest.main()