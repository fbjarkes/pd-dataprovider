#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import click
import pandas as pd
from qa_dataprovider import AsyncIBDataProvider


def download_years(tickers: str, years: str, host: str, port: int, timeout: int):
    """
    TICKER # Stock type and SMART exchange

    TICKER-STK # Stock and SMART exchange

    TICKER-STK-EXCHANGE # Stock

    TICKER-STK-EXCHANGE-CURRENCY # Stock

    TICKER-CFD # CFD and SMART exchange

    TICKER-CFD-EXCHANGE # CFD

    TICKER-CDF-EXCHANGE-CURRENCY # Stock

    TICKER-IND-EXCHANGE # Index

    TICKER-IND-EXCHANGE-CURRENCY # Index

    TICKER-YYYYMM-EXCHANGE # Future

    TICKER-YYYYMM-EXCHANGE-CURRENCY # Future

    TICKER-YYYYMM-EXCHANGE-CURRENCY-MULT # Future

    TICKER-FUT-EXCHANGE-CURRENCY-YYYYMM-MULT # Future

    TICKER-YYYYMM-EXCHANGE-CURRENCY-STRIKE-RIGHT # FOP

    TICKER-YYYYMM-EXCHANGE-CURRENCY-STRIKE-RIGHT-MULT # FOP

    TICKER-FOP-EXCHANGE-CURRENCY-YYYYMM-STRIKE-RIGHT # FOP

    TICKER-FOP-EXCHANGE-CURRENCY-YYYYMM-STRIKE-RIGHT-MULT # FOP

    CUR1.CUR2-CASH-IDEALPRO # Forex

    TICKER-YYYYMMDD-EXCHANGE-CURRENCY-STRIKE-RIGHT # OPT

    TICKER-YYYYMMDD-EXCHANGE-CURRENCY-STRIKE-RIGHT-MULT # OPT

    TICKER-OPT-EXCHANGE-CURRENCY-YYYYMMDD-STRIKE-RIGHT # OPT

    TICKER-OPT-EXCHANGE-CURRENCY-YYYYMMDD-STRIKE-RIGHT-MULT # OPT

    :param ticker:
    :param years:
    :return:
    """
    ib = AsyncIBDataProvider(host, port, timeout)
    ib.connect()

    for ticker in tickers.split(','):
        total = pd.DataFrame()
        for i, y in enumerate(years.split(',')):
            try:
                df = ib.get_data([ticker], f'{y}-01-01', f'{y}-12-31', keep_alive=True)
                total = total.append(df)
            except Exception as e:
                print(f"Error for {y}: '{e}'. Stopping.")
                break

        name = f"{total['Ticker'].iloc[0]}"
        total = total.sort_index(ascending=True)
        print(f"Writing {len(total)} rows to {name}.csv")
        total.to_csv(f"{name}.csv", header=True)

    ib.disconnect()

def download(file, timeframe, transform):
    ib = AsyncIBDataProvider()
    with open(file) as f:
        tickers = list(filter(lambda x: len(x) > 0 and x[0] != '#', [ticker.rstrip() for ticker in
                                                          f.readlines()]))

    df_list = ib.get_data(tickers, from_date=None, to_date=None,
                          timeframe=timeframe,
                          transform=transform)

    for i, df in enumerate(df_list):
        if len(df) > 0:
            name = f"{df['Ticker'].iloc[0]}"
            print(f"Writing {len(df)} rows to {name}.csv")
            df.to_csv(f"{name}.csv", header=True,
                      columns=['Open','High','Low','Close','Volume','Ticker'])
        else:
            print(f"No data for '{tickers[i]}'")


if __name__ == '__main__':
    download(None, "5min",'5min')


