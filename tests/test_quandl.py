#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import unittest
import logging


import pandas as pd

from qa_dataprovider.generic_dataprovider import GenericDataProvider
from qa_dataprovider.quandl_dataprovider import QuandlDataProvider


class TestQuandl(unittest.TestCase):

    provider = QuandlDataProvider(["data"])

    def test_daily_trading_days(self):
        daily = self.provider.get_data(['DIS'],'2010-01-01','2017-01-01')[0]

        assert daily.loc['2016-01-04']['Day'] == 1 # Monday Jan. 4
        assert daily.loc['20160108']['Day'] == 5 # Friday Jan. 8
        assert daily.loc['20160111']['Day'] == 6  # Monday Jan. 11
        assert daily.loc['20161230']['Day'] == 252  #240? Friday Dec. 30


if __name__ == '__main__':
    unittest.main()