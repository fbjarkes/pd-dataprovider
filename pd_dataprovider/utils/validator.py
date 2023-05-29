# -*- coding: utf-8; py-indent-offset:4 -*-
import logging as logger

import pandas as pd
import pandas_market_calendars as mcal


class Validator:
    logger.basicConfig(level=logger.INFO, format='%(filename)s: %(message)s')

    def __init__(self):
        self.nyse = mcal.get_calendar('NYSE')

    def validate_nan(self, df, ticker):
        nan_rows = pd.isnull(df).any(axis=1).to_numpy().nonzero()[0]
        #nan_rows = pd.isnull(df).any(1)
        if len(nan_rows) > 0:
            logger.warning("WARNING: {:s} has {:d} rows with NaN".format(ticker, len(nan_rows)))

    @DeprecationWarning
    def validate_dates(self, data, ticker, from_date, to_date):
        nyse_days = len(self.nyse.valid_days(start_date=from_date, end_date=to_date))
        if len(data) == 0:
            raise Exception("No data available from {} to {}. Verify {} data by "
                            "provider includes these dates.".format(ticker, from_date, to_date, data))

        if ((nyse_days / len(data)) - 1) > 0.2:
            logger.warning("WARNING: {:s} has only {:d} rows, expecting approx. {:d} rows"
                           .format(ticker, len(data), nyse_days))
    @DeprecationWarning
    def validate_timeframe(self, df, timeframe):
        delta = (df.index[1] - df.index[0])
        if delta.seconds == 300 and timeframe != '5min':
            raise Exception(f"Timeframe missmatch: expected '{timeframe}' but have {delta.seconds} "
                            f"seconds")

