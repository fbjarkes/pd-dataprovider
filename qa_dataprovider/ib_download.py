#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

from qa_dataprovider import AsyncIBDataProvider


def download(file, timeframe, transform):
    ib = AsyncIBDataProvider()
    with open(file) as f:
        tickers = list(filter(lambda x: x[0] != '#',[ticker.rstrip() for ticker in f.readlines()]))

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


