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

from qa_dataprovider.generic_dataprovider import GenericDataProvider

#TODO: use wrapper framework like https://github.com/ranaroussi/ezibpy?
class IBDataProvider(GenericDataProvider):
    def _get_data_internal(self, ticker, from_date, to_date, timeframe):
        pass

    def add_quotes(self, data, ticker):
        pass