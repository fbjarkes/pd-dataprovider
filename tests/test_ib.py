#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-
import unittest

from ib_insync import Stock, Index, Forex, Future
from qa_dataprovider import AsyncIBDataProvider


class TestIb(unittest.TestCase):

    def assert_contract(self, contract,
                        expect_contract_type, expect_ticker,
                        expect_exchange, expect_currency,
                        expect_expire, multiplier=None):
        assert isinstance(contract, expect_contract_type)
        print(contract)
        assert contract.symbol == expect_ticker
        assert contract.exchange == expect_exchange
        assert contract.currency == expect_currency
        if expect_expire:
            assert contract.lastTradeDateOrContractMonth == expect_expire
        if multiplier:
            assert contract.multiplier == multiplier

    def test_parse_contract(self):
        self.assert_contract(AsyncIBDataProvider.parse_contract("SPY"),
                             Stock, "SPY","ARCA","USD","")
        self.assert_contract(AsyncIBDataProvider.parse_contract("BA-NYSE"),
                             Stock, "BA", "NYSE", "USD","")
        self.assert_contract(AsyncIBDataProvider.parse_contract("HM.B-SFB-SEK"),
                             Stock, "HM.B", "SFB", "SEK","")
        self.assert_contract(AsyncIBDataProvider.parse_contract("OMXS30-IND-OMS-SEK"),
                             Index, "OMXS30", "OMS", "SEK", "")
        self.assert_contract(AsyncIBDataProvider.parse_contract("USD.JPY-CASH-IDEALPRO"),
                             Forex, "USD", "IDEALPRO", "JPY", "")
        self.assert_contract(AsyncIBDataProvider.parse_contract("ES-201709-GLOBEX"),
                             Future, "ES", "GLOBEX", "USD", "201709")
        self.assert_contract(AsyncIBDataProvider.parse_contract("OMXS30-201708-OMS-SEK"),
                             Future, "OMXS30", "OMS", "SEK", "201708")
        self.assert_contract(AsyncIBDataProvider.parse_contract("DAX-201709-DTB-EUR-25"),
                             Future, "DAX", "DTB", "EUR", "201709", "25")




if __name__ == '__main__':
    unittest.main()