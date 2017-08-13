#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-


import click
import pandas as pd

from qa_dataprovider import AsyncIBDataProvider



#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-


import click
import pandas as pd

from qa_dataprovider import AsyncIBDataProvider


#@click.command()
@click.option('--file', type=click.Path(exists=True), help="Read tickers from file")
@click.option('--timeframe')
def main(file, timeframe):

    ib = AsyncIBDataProvider()

    #tickers = []
    #with open(file) as f:
    #    tickers = [ticker.rstrip() for ticker in f.readlines()]

    tickers = ['SPY']
    df_list = ib.get_data(tickers, from_date=None, to_date=None, timeframe=timeframe)

    for i, df in enumerate(df_list):
        if len(df) > 0:
            #last_date = df.index[-1].name
            #name = f"{df[0]['Ticker']}_{last_date}"

            name = f"{df[0]['Ticker']}"
            print(f"Writing {len(df)} rows to {name}.csv")
            df.to_csv(f"{name}.csv", header=True)
        else:
            print(f"No data for '{tickers[i]}'")


if __name__ == '__main__':
    #main()

    main(None, "5min")


