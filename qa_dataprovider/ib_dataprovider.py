#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import logging as logger
from datetime import datetime, timedelta


import dateutil.parser as dp
import numpy as np
import pandas as pd
import pandas_datareader
import pandas_datareader.data as web
import requests_cache
from pytz import timezone

# TODO: check if it only imports when using this class
#import ibapi
#from ib_insync import *

from qa_dataprovider.generic_dataprovider import GenericDataProvider

#TODO: use wrapper framework like https://github.com/ranaroussi/ezibpy?
class IBDataProvider(GenericDataProvider):

    CLIENT_ID = 0

    @staticmethod
    def get_unique_id():
        IBDataProvider.CLIENT_ID += 1
        return IBDataProvider.CLIENT_ID

    def __init__(self):
        self.client_id = IBDataProvider.get_unique_id()
        ib = IB()
        ib.connect('127.0.0.1', 7497, clientId=1)

    def _get_data_internal(self, ticker, from_date, to_date, timeframe):
        pass

    def add_quotes(self, data, ticker):
        pass



if __name__ == '__main__':
    ib = IBDataProvider()

    dailys = ib.get_data(['SPY'], '2010-01-01', '2016-12-31',
                               max_workers=5, timeframe='week')
    print(dailys[0].tail())
    print(dailys[1].tail())