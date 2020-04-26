import unittest

from qa_dataprovider.providers.async_ib_dataprovider import AsyncIBDataProvider


class TestIb(unittest.TestCase):

    def test_parse_contract(self):
        assert AsyncIBDataProvider.exctract_symbol('SPY') == tuple(['STK', 'SPY', 'ARCA', 'USD', '', ''])
        assert AsyncIBDataProvider.exctract_symbol('BA-NYSE') == tuple(['STK', 'BA', 'NYSE', 'USD', '', ''])
        assert AsyncIBDataProvider.exctract_symbol('HM.B-SFB-SEK') == tuple(['STK', 'HM.B', 'SFB', 'SEK', '', ''])
        assert AsyncIBDataProvider.exctract_symbol('OMXS30-IND-OMS-SEK') == tuple(['IND', 'OMXS30', 'OMS', 'SEK', '', ''])
        assert AsyncIBDataProvider.exctract_symbol('USD.JPY-CASH-IDEALPRO') == tuple(['FX', 'USDJPY', 'IDEALPRO', 'CASH', '', ''])
        assert AsyncIBDataProvider.exctract_symbol('ES-201709-GLOBEX') == tuple(['FUT', 'ES', 'GLOBEX', 'USD', '201709', ''])
        assert AsyncIBDataProvider.exctract_symbol('OMXS30-201708-OMS-SEK') == tuple(['FUT', 'OMXS30', 'OMS', 'SEK', '201708', ''])
        assert AsyncIBDataProvider.exctract_symbol('DAX-201709-DTB-EUR-25') == tuple(['FUT', 'DAX', 'DTB', 'EUR', '201709', '25'])


if __name__ == '__main__':
    unittest.main()