#!/usr/bin/env python
import click
import pandas as pd
from qa_dataprovider.provider_factory import ProviderFactory
from qa_dataprovider.providers.async_ib_dataprovider import AsyncIBDataProvider
from qa_dataprovider.objects import SymbolData


def download(symbols, file, timeframe, verbose, tz='America/New_York'):
    ib = ProviderFactory.make_provider('alpaca', verbose=verbose, tz=tz, limit=1000)
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
    download(symbols, file, timeframe, verbose, tz)


if __name__ == '__main__':
    main()
