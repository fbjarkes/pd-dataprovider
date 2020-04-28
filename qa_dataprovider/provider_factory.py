import os
from configparser import ConfigParser
from qa_dataprovider.providers.async_ib_dataprovider import AsyncIBDataProvider
from qa_dataprovider.providers.csv_dataprovider import CsvFileDataProvider


class ProviderFactory:

    @staticmethod
    def make_provider(provider :str, verbose: int = 0, **kwargs):
        cfg = ConfigParser()
        cwd = os.path.dirname(os.path.realpath(__file__))
        cfg.read(f"{os.path.split(cwd)[0]}/qa_dataprovider.ini")

        if provider == 'ibasync':
            return AsyncIBDataProvider(verbose=verbose)

        elif provider in ['ibfile', 'quandl', 'csv']:
            return CsvFileDataProvider(cfg[provider]['paths'].split())

        elif provider == 'tradingview':
            return CsvFileDataProvider(
                cfg[provider]['paths'].split(),
                col_names=['time','open','high','low','close', 'volume'],
                epoch=True
            )
        elif provider == 'avfile':
            return CsvFileDataProvider(
                cfg[provider]['paths'].split(),
                col_names=['timestamp','open','high','low','close', 'volume'],
                epoch=False
            )
        elif provider == 'infront':
            return CsvFileDataProvider(
                cfg[provider]['paths'].split(),
                prefix=['NSQ', 'NYS', 'NYSF', 'SSE', ''] if kwargs.get('prefix') is None else kwargs.get('prefix')
            )

        raise Exception(f"Invalid provider {provider}")