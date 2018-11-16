import aiohttp
import asyncio
import requests
import pandas as pd
from urllib.parse import quote


def get_data(symbols, from_='2017-1-1', to='2018-1-1'):
    r"""Sync version of a_get_data"""
    return asyncio.run((a_get_data(symbols, from_, to)))


async def a_get_data(symbols, from_='2017-1-1', to='2018-1-1', chunk_size=50, chunk_delay=20):
    r"""Obtain data for given companies and time range

    :param symbols: list of company symbols
    :param from_: str reasonably formatted date
    :param to: str reasonably formatted date
    :param chunk_size: int # of companies to include in one batch of requests
    :param chunk_delay: int delay in seconds between batches 
    :return: pd.DataFrame:
    :rtype: pd.DataFrame
    """
    res = requests.get('https://api.iextrading.com/1.0/ref-data/symbols')  # known symbols data
    known_symbols = set(x['symbol'] for x in res.json())
    _symbols = [s for s in symbols if s in known_symbols]
    data = list()
    while _symbols:
        _data = await asyncio.gather(*[_a_get_data(s, from_, to) for s in _symbols[:chunk_size]])
        data.extend(_data)
        del _symbols[:chunk_size]
        await asyncio.sleep(chunk_delay)

    data = dict(zip(symbols, data))
    data = pd.DataFrame.from_dict(data)
    return data


async def _a_get_data(symbol, from_='2017-1-1', to='2018-1-1'):
    from_ts = pd.Timestamp(from_)
    now_ts  = pd.Timestamp.now()
    years_to_fetch = int((now_ts - from_ts).days/360) + 1
    range_ = str(years_to_fetch) + 'y'
    data = await get_price_data(symbol, range_)
    return format_price_data(data, from_, to)


async def get_price_data(symbol, range='2y'):
    r"""Fetch day prices for given company and time range from iextrading.com

    :param symbol: str Company symbol
    :param range: str Desired range, e.g. 1d, 1m, 1y, 2y
    :return: json: as specified in https://iextrading.com/developer/docs/#chart
    :rtype: json
    """

    url = 'https://api.iextrading.com/1.0/stock/{}/chart/{}'.format(quote(symbol), range)
    while True:
        try:
            async with aiohttp.ClientSession() as ses:
                async with ses.get(url) as res:
                    return await res.json()
        except aiohttp.ClientError:
            pass


def format_price_data(data, from_='2017-1-1', to='2018-1-1'):
    r"""Extract closing price, filter and index by time, and create Series

    :param data: as returned by get_price_data
    :param from_: str reasonably formatted date
    :param to: str reasonably formatted date
    :return: pd.Series:
    :rtype: pd.Series
    """

    df = pd.DataFrame.from_records(data)
    df['timelabel'] = df.label.apply(lambda x: x if len(x.split(' ')) == 3 else x + ', 18')  # repair labels with missing year
    df['time'] = [pd.Timestamp(x) for x in df.timelabel]
    df = df[df.time.between(pd.Timestamp(from_), pd.Timestamp(to))]
    ts = df.set_index('time')['close']
    return ts