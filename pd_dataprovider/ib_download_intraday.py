#!/usr/bin/env python

import argparse
import logging

import pandas as pd
# TODO: import with "." ?
from pd_dataprovider.provider_factory import ProviderFactory
from pd_dataprovider.providers.async_ib_dataprovider import AsyncIBDataProvider
from pd_dataprovider.objects import SymbolData


def download_intraday(symbols, file, timeframe, verbose, start, tz='America/New_York', id=0, host='127.0.0.1', port=7498):
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
    ib = ProviderFactory.make_provider('ibasync', verbose=verbose, keep_alive=True, tz=tz, host=host, port=port)
    symbols = symbols.split(',')
    if file:
        with open(file) as f:
            symbols = [ticker.rstrip() for ticker in f.readlines() if not ticker.startswith('#')]
    chunks = [symbols[i:i + 3] for i in range(0, len(symbols), 3)]
    for chunk in chunks:
        datas = ib.get_datas([SymbolData(symbol, timeframe, timeframe, start, '', True) for symbol in chunk])
        for symbol, data in zip(chunk, datas):
            data.df.to_csv(f"{symbol}.csv", header=True)
            print(f"Wrote {len(data.df)} rows to {symbol}.csv")


def main():
    logging.getLogger('ib_insync').setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbols', default="SPY", help="Comma separated list of symbols", dest='symbols')
    parser.add_argument('--file', help='Read symbols from file', dest='file')
    parser.add_argument('--timeframe', default='5min', dest='timeframe')
    parser.add_argument('-v', '--verbose', action='count', dest='verbose')
    parser.add_argument('--start', dest='start')
    parser.add_argument('--tz', default='America/New_York', dest='tz')
    parser.add_argument('--id', default='0', dest='id')
    parser.add_argument('--host', default='127.0.0.1', dest='host')
    parser.add_argument('--port', default=7498, type=int, dest='port')

    args = parser.parse_args()
    download_intraday(args.symbols, args.file, args.timeframe, args.verbose, args.start, args.tz, args.id, args.host, args.port)


if __name__ == '__main__':
    main()

