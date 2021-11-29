import os
from configparser import ConfigParser
from qa_dataprovider.providers.async_ib_dataprovider import AsyncIBDataProvider
from qa_dataprovider.providers.csv_dataprovider import CsvFileDataProvider
from qa_dataprovider.providers.json_dataprovider import JSONDataProvider


class ProviderFactory:

    @staticmethod
    def make_provider(provider :str, verbose: int = 0, **kwargs):
        cfg = ConfigParser()
        cwd = os.path.dirname(os.path.realpath(__file__))
        cfg.read(f"{os.path.split(cwd)[0]}/qa_dataprovider.ini")

        paths = cfg[provider]['paths'].split()
        if 'paths' in kwargs:
            paths = kwargs['paths']
        # TODO: get config path from env (if deployed in cloud for instance)

        if provider == 'ibasync':
            return AsyncIBDataProvider(verbose=verbose,
                                       host=cfg[provider]['host'],
                                       port=int(cfg[provider]['port']),
                                       timeout=int(cfg[provider]['timeout']),
                                       chunk_size=int(cfg[provider]['chunk_size']),
                                       **kwargs)

        elif provider in ['ibfile', 'quandl', 'csv','ibfile-intraday']:
            return CsvFileDataProvider(paths, verbose=verbose)
        elif provider == 'tradingview':
            return CsvFileDataProvider(
                paths,
                verbose=verbose,
                col_names=['time','open','high','low','close', 'volume'],
                epoch=True
            )
        elif provider == 'avfile':
            return CsvFileDataProvider(
                paths,
                verbose=verbose,
                col_names=['timestamp','open','high','low','close', 'volume'],
                epoch=False
            )
        elif provider == 'infront':
            return CsvFileDataProvider(
                paths,
                verbose=verbose,
                prefix=['NSQ', 'NYS', 'NYSF', 'SSE', ''] if kwargs.get('prefix') is None else kwargs.get('prefix')
            )
        elif provider == 'alpaca':
            return JSONDataProvider(
                paths,
                verbose=verbose,
                keys=['t', 'o', 'h', 'l', 'c', 'v'],
                epoch=True
            )
        elif provider == 'alpaca-file':
            return JSONDataProvider(
                paths,
                verbose=verbose,
                keys=['startEpochTime', 'openPrice', 'highPrice', 'lowPrice', 'closePrice', 'volume'],
                epoch=True
            )
        raise Exception(f"Invalid provider {provider}")
