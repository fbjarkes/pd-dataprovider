#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-


import click
import pandas as pd

from qa_dataprovider import AsyncIBDataProvider


@click.command()
@click.option('--years', default="2017,2016,2015,2014,2013,2012,2011,2010,2009,2008,2007,2006,2005,"
                                 "2004,2003,2002,2001,2000", help="Comma separated list of years",
              show_default=True)
@click.option('--tickers', default="SPY", help="Comma separated list of tickers.",
              show_default=True)
def main(tickers: str, years: str):
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

    :param tickers:
    :param years:
    :return:
    """
    ib = AsyncIBDataProvider()

    total = pd.DataFrame()
    for i, y in enumerate(years.split(',')):
        try:
            df = ib.get_data([tickers], f'{y}-01-01', f'{y}-12-31')
        except Exception as e:
            print(f"Error for {y}: '{e}'. Writing file.")
            break
        else:
            total = total.append(df)

    total = total.sort_index(ascending=True)
    print(f"Writing {len(total)} rows to {tickers}.csv")
    total.to_csv(f"{tickers}.csv", header=True)


if __name__ == '__main__':
    #main(tickers="EUR.USD-CASH-IDEALPRO", years="2016,2015")
    main()


