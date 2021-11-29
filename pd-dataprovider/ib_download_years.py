#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-

import click
import pandas as pd
from qa_dataprovider.provider_factory import ProviderFactory
from qa_dataprovider.providers.async_ib_dataprovider import AsyncIBDataProvider
from qa_dataprovider.objects import SymbolData


def download_years(symbols: str, file: str, years: str, verbose: int):
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
    """
    ib = ProviderFactory.make_provider('ibasync', verbose=verbose, keep_alive=True)
    symbols = symbols.split(',')
    if file:
        with open(file) as f:
            symbols = [ticker.rstrip() for ticker in f.readlines() if not ticker.startswith('#')]

    for symbol in symbols:
        total = pd.DataFrame()
        for i, y in enumerate(years.split(',')):
            try:
                df_list = ib.get_dataframes(
                    [SymbolData(symbol, 'day', 'day', f'{y}-01-01', f'{y}-12-31')])
                if df_list[0].empty:
                    print(f"No data for {y}. Stopping")
                    break
                else:
                    total = total.append(df_list[0])
            except Exception as e:
                print(f"Error for {y}: '{e}'. Stopping.")
                break


        total = total.sort_index(ascending=True)
        if len(total) > 0:
            print(f"Writing {len(total)} rows to {symbol}.csv")
            total.to_csv(f"{symbol}.csv", header=True)
        else:
            print(f"No data found for {symbol}")




@click.command()
@click.option('--years', default="2020,2019,2018,2017,2016,2015,2014,2013,2012,2011,2010,2009,2008,2007,2006,2005,2004,2003,2002,2001,2000,1999,1998,1997,1996,1995,1994,1993,1992,1991,1990,1989,1988,1987,1986,1985,1984", help="Comma separated list of years")
@click.option('--symbols', default="SPY", help="Comma separated list of symbols")
@click.option('--file', type=click.Path(exists=True), help='Read symbols from file')
@click.option('-v', '--verbose', count=True)
def main(years, symbols, file, verbose):
    download_years(symbols, file, years, verbose)


if __name__ == '__main__':
    main()
