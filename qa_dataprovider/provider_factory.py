import os

from qa_dataprovider.providers.async_ib_dataprovider import AsyncIBDataProvider
from qa_dataprovider.providers.csv_dataprovider import CsvFileDataProvider
from qa_dataprovider.providers.sql_dataprovider import SQLDataProvider

AVAILABLE_PROVIDERS = ['ibasync', 'ibfile', 'sql', 'quandl', 'infront', 'csv', 'tradingview']


class ProviderFactory:

    @staticmethod
    def make_provider(provider :str, verbose: int = 0, get_quotes: bool = False, **kwargs):
        home = ''
        if 'HOME' in os.environ:
            home = f"{os.environ['HOME']}"
        if 'USERPROFILE' in os.environ:
            home = f"{os.environ['USERPROFILE']}"

        if provider == 'ibasync':
            return AsyncIBDataProvider(verbose=verbose)

        elif provider == 'ibfile':
            return CsvFileDataProvider(
                [
                    f"ibfile",
                    f"{home}/ibfile",
                    f"{home}/Dropbox/ibfile",
                ])

        elif provider == 'quandl':
            return CsvFileDataProvider(
                [
                    f"quandl/iwm",
                    f"{home}/quandl/iwm",
                    f"{home}/Dropbox/quandl/iwm",
                    f"quandl/spy",
                    f"{home}/quandl/spy",
                    f"{home}/Dropbox/quandl/spy",
                    f"quandl/ndx",
                    f"{home}/quandl/ndx",
                    f"{home}/Dropbox/quandl/ndx",
                    f"quandl/chris",
                    f"{home}/quandl/chris",
                    f"{home}/Dropbox/quandl/chris"

                ])
        elif provider == 'csv':
            return CsvFileDataProvider(
                [
                    f"csv",
                    f"{home}/csv",
                    f"{home}/Dropbox/csv",
                ])
        elif provider == 'tradingview':
            return CsvFileDataProvider(
                [
                    f"csv",
                    f"{home}/csv",
                    f"{home}/Dropbox/csv",
                ],
                col_names=['time','open','high','low','close', 'volume'],
                epoch=True
            )
        elif provider == 'alphavantage':
            return CsvFileDataProvider(
                [
                    f"csv",
                    f"{home}/csv",
                    f"{home}/Dropbox/alphavantage/csv",
                ],
                col_names=['timestamp','open','high','low','close', 'volume'],
                epoch=False
            )
        elif provider == 'infront':
            return CsvFileDataProvider(
                [
                    f"infront",
                    f"{home}/infront",
                    f"{home}/Dropbox/infront"
                ],
                prefix=['NSQ', 'NYS', 'NYSF', 'SSE', ''] if kwargs.get('prefix') is None else kwargs.get('prefix')
            )
        elif provider == 'sql':
            return SQLDataProvider(
                [
                    f"sql/tickdata_5min.db",
                    f"{home}/sql/tickdata_5min.db",
                    f"{home}/Dropbox/sql/tickdata_5min.db",
                ])

        raise Exception(f"Invalid provider {provider}")

