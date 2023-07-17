import os
from configparser import ConfigParser
from pd_dataprovider.providers.async_ib_dataprovider import AsyncIBDataProvider
from pd_dataprovider.providers.csv_dataprovider import CsvFileDataProvider
from pd_dataprovider.providers.json_dataprovider import JSONDataProvider


class ProviderFactory:

    @staticmethod
    def make_provider(provider :str, verbose: int = 0, **kwargs):
        """
        Create an instance of data provider. Optional configuration provided as dict.
            Example:
                CONFIG = {
                    'alpaca-file-v2': {'paths': '/Users/fbjarkes/Bardata/alpaca-v2'},
                    'tradingview': {'paths': '/Users/fbjarkes/Bardata/tradingview'}
                }
        """
        cfg = ConfigParser()
        cwd = os.path.dirname(os.path.realpath(__file__))
        cfg.read(f"{os.path.split(cwd)[0]}/pd_dataprovider.ini") # TODO: override cfg path from env-variable

        # Can override paths from cfg with parameter
        paths = []
        if provider in cfg and 'paths' in cfg[provider]:
            paths = cfg[provider]['paths'].split(',')
        if provider in kwargs and 'paths' in kwargs[provider]:
            paths = kwargs[provider]['paths'].split(',')

        if provider == 'ibasync':
                # return AsyncIBDataProvider(verbose=verbose,
                #                        host=cfg[provider]['host'],
                #                        port=int(cfg[provider]['port']),
                #                        timeout=int(cfg[provider]['timeout']),
                #                        chunk_size=int(cfg[provider]['chunk_size']),
                #                        **kwargs)
                return AsyncIBDataProvider(verbose=verbose, **kwargs, timeout=60, chunk_size=10)

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
                epoch=True,
                **kwargs
            )
        elif provider == 'alpaca-file-v2':
            return JSONDataProvider(
                paths,
                verbose=verbose,
                keys=['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume'],
                **kwargs
            )
        raise Exception(f"Invalid provider {provider}")
