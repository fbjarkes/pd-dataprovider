#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

from qa_dataprovider.async_ib_dataprovider import AsyncIBDataProvider
from qa_dataprovider.csv_dataprovider import CsvFileDataProvider
from qa_dataprovider.web_dataprovider import CachedWebDataProvider

AVAILABLE_PROVIDERS = ['google', 'yahoo','ibasync', 'ibfile','sql','quandl','infront']


class Factory:

    @staticmethod
    def make_provider( provider, clear_cache=False, get_quotes=False, **kwargs):
        if provider == 'ibasync':
            return AsyncIBDataProvider()

        elif provider == 'ibfile':
            return CsvFileDataProvider(
                [
                    '../../ibfile',
                    '../ibfile',
                ])

        elif provider == 'quandl':
            return CsvFileDataProvider(
                [
                    '../../quandl/iwm',
                    '../quandl/iwm',
                    '../../quandl/spy',
                    '../quandl/spy',
                    '../../quandl/ndx',
                    '../quandl/ndx'
                ])

        elif provider == 'infront':
            return CsvFileDataProvider(
                [
                    '../../infront',
                    '../infront'
                ],
                prefix=['NSQ', 'NYS', 'NYSF', 'SSE', ''] if kwargs.get('prefix') is None else kwargs.get('prefix')
            )

        elif provider == 'nasdaq':
            return CsvFileDataProvider(
                [
                    '../../nasdaq',
                    '../infront'
                ])

        elif provider == 'sql':
            raise Exception("Not implemented yet")

        elif provider == 'ig':
            raise Exception("Not implemented yet")

        else:
            return CachedWebDataProvider(provider, expire_days=0, get_quotes=get_quotes)

