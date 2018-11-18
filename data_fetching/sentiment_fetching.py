import gzip
import os
import pandas as pd
import twitterscraper as ts
from glob import glob


def query(query, n=0, since='2017-01-01', until='2018-01-01', lang='en', poolsize=30):
    r"""Query at least n tweets per day in [since, until] range.

    :param query: twitter query string
    :param n: minimum # of tweets to fetch
    :param since: reasonably formatted date
    :param until: reasonably formatted date
    :param lang: ISO code for tweet language
    :param poolsize: # of processes to start for fetching tweets (default 30)
    :return:
    """

    days = list(pd.date_range(start=since, end=until, freq='D'))
    result = []
    while len(days) > 1:
        begindate = days[0].to_pydatetime().date()
        enddate = days[:poolsize][-1].to_pydatetime().date()
        limit_ = n*len(days[:poolsize])
        result_ = ts.query_tweets(query, limit=limit_, poolsize=poolsize, lang=lang,
                                  begindate=begindate, enddate=enddate)
        result_ = [x.__dict__ for x in result_]
        for x in result_:
            del x['html']

        result.extend(result_)
        del days[:poolsize]

    return result


def fetch_all(companies, queries, dir, overwrite=False, poolsize=30):
    r"""Execute queries and save results.

    :param companies: S&P Companies data
    :param queries: Twitter queries corresponding to companies
    :param dir: Path to directory for saving tweets
    :param overwrite: Set True to overwrite previously saved tweets (dafault False)
    :param poolsize: # of processes to start for fetching tweets (default 30)
    :return: None
    """
    os.makedirs(dir, exist_ok=True)
    for c, q in zip(companies.itertuples(), queries):
        if not overwrite and glob(os.path.join(dir, c.Symbol+'.csv.gz')):
            continue
        tweets = query(**q, poolsize=poolsize)
        df = pd.DataFrame.from_records(tweets)
        df.to_csv(gzip.open(os.path.join(dir, c.Symbol+'.csv.gz'), 'wt'))
        print('Fetched {} tweets for {}[{}]'.format(len(tweets), c.Security, c.Symbol))
