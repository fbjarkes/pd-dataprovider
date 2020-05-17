#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-

import click
import pandas as pd
from qa_dataprovider.provider_factory import ProviderFactory
from qa_dataprovider.providers.async_ib_dataprovider import AsyncIBDataProvider
from qa_dataprovider.objects import SymbolData


def download_intraday(symbols, file, timeframe, verbose, tz='America/New_York'):
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
    ib = ProviderFactory.make_provider('ibasync', verbose=verbose, keep_alive=True, tz=tz)
    symbols = symbols.split(',')
    if file:
        with open(file) as f:
            symbols = [ticker.rstrip() for ticker in f.readlines() if not ticker.startswith('#')]
    chunks = [symbols[i:i + 3] for i in range(0, len(symbols), 3)]
    for chunk in chunks:
        datas = ib.get_datas([SymbolData(symbol, timeframe, timeframe, '', '', True) for symbol in chunk])
        for symbol, data in zip(chunk, datas):
            data.df.to_csv(f"{symbol}.csv", header=True)
            print(f"Wrote {len(data.df)} rows to {symbol}.csv")


@click.command()
@click.option('--symbols', default="SPY", help="Comma separated list of symbols", show_default=True)
@click.option('--file', type=click.Path(exists=True), help='Read symbols from file')
@click.option('--timeframe', default='5min')
@click.option('-v', '--verbose', count=True)
@click.option('--tz', default='America/New_York')
def main(symbols, file, timeframe, verbose, tz):
    download_intraday(symbols, file, timeframe, verbose, tz)


if __name__ == '__main__':
    main()
