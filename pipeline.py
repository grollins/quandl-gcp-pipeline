from os import environ
from datetime import datetime, timedelta
import logging
import requests
import backoff

import google.auth
from google.cloud import storage

import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
plt.style.use('seaborn-deep')

from numpy import percentile
import pandas as pd
import pymc3 as pm

import luigi
from luigi.contrib import gcs
from luigi.contrib import bigquery
from luigi.contrib import external_program


# Google Cloud
PROJECT_ID = 'senpai-io'
BUCKET_NAME = 'senpai-io.appspot.com'
BUCKET_PATH = 'gs://{}'.format(BUCKET_NAME)
BUCKET_SUBDIR = 'quandl-stage'
CREDENTIALS, _ = google.auth.default()
GCS_CLIENT = gcs.GCSClient(CREDENTIALS)
GCS_BUCKET = storage.Client().get_bucket(BUCKET_NAME)
BQ_CLIENT = bigquery.BigQueryClient(CREDENTIALS)

# Dates
TODAY = datetime.today()
YESTERDAY = TODAY - timedelta(days=1)

# Logging
logger = logging.getLogger('luigi-interface')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('logs/{date:%Y-%m-%d}-luigi.log'.format(date=YESTERDAY))
fh.setLevel(logging.INFO)
logger.addHandler(fh)


class GetDailyStockData(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return []

    def output(self):
        output_path_template = '{}/{}/data/{date:%Y-%m-%d}.csv'
        output_path = output_path_template.format(BUCKET_PATH, BUCKET_SUBDIR, date=self.date)
        return gcs.GCSTarget(output_path, client=GCS_CLIENT)

    def run(self):
        ticker_df = pd.read_csv('djia_symbols.csv')
        ticker_list = ticker_df.symbol.tolist()

        session = requests.Session()

        @backoff.on_exception(backoff.constant,
                      (requests.exceptions.RequestException),
                      jitter=backoff.random_jitter,
                      max_tries=5,
                      interval=30)
        def send_iex_request(url):
            response = session.get(url=url)
            response.raise_for_status()
            return response

        IEX_URL = 'https://api.iextrading.com/1.0/stock/market/batch?symbols={}&types=ohlc'
        url = IEX_URL.format(','.join(ticker_list))
        r = send_iex_request(url)
        price = [(k, v['ohlc']['close']['price']) for k,v in r.json().items()]
        df = pd.DataFrame(price, columns=['ticker', 'price'])
        df['date'] = self.date.strftime('%Y-%m-%d')

        with self.output().open('w') as out_file:
            df.to_csv(out_file, index=False)


class GenerateReport(luigi.Task):
    date = luigi.DateParameter(default=YESTERDAY)

    def requires(self):
        return GetDailyStockData(self.date)

    def output(self):
        output_path_template = '{}/{}/report/{date:%Y-%m-%d}.txt'
        output_path = output_path_template.format(BUCKET_PATH, BUCKET_SUBDIR, date=self.date)
        return gcs.GCSTarget(output_path, client=GCS_CLIENT)

    def run(self):
        with self.input().open('r') as in_file:
            df = pd.read_csv(in_file)

        with pm.Model() as model:
            # parameters
            alpha = pm.Uniform('alpha', 0, 100)
            beta = pm.Uniform('beta', 0, 1)
            # observed data
            p = pm.Gamma('p', alpha=alpha, beta=beta, observed=df['price'])
            # run sampling
            trace = pm.sample(2000, tune=1000)

        ppc = pm.sample_ppc(trace, samples=200, model=model, size=30)

        num_bins = df.shape[0]
        plt.figure()
        ax = df['price'].hist(bins=num_bins, cumulative=True, normed=1, histtype='step',
                              color='k')

        for i in range(ppc['p'].shape[0]):
            sA = pd.Series(ppc['p'][i,:])
            sA.hist(bins=num_bins, cumulative=True, normed=1, histtype='step', ax=ax,
                    color='r', alpha=0.1)

        local_png_path_template = 'output/{date:%Y-%m-%d}.png'
        local_png_path = local_png_path_template.format(date=self.date)
        plt.savefig(local_png_path)

        remote_png_path_template = '{}/report/{date:%Y-%m-%d}.png'
        remote_png_path = remote_png_path_template.format(BUCKET_SUBDIR, date=self.date)
        GCS_BUCKET.blob(remote_png_path).upload_from_filename(filename=local_png_path)

        with self.output().open('w') as out_file:
            out_file.write(str(df['price'].mean()))


class DeployAppEngine(external_program.ExternalProgramTask):
    date = luigi.DateParameter(default=YESTERDAY)

    def requires(self):
        return GenerateReport(self.date)

    def output(self):
        output_template = 'output/{date:%Y-%m-%d}_appeng.txt'
        output_path = output_template.format(date=self.date)
        return luigi.LocalTarget(output_path)

    def program_args(self):
        return ['./deploy_app_engine.sh']


'''
class LoadRecordsInTable(bigquery.BigQueryLoadTask):
    date = luigi.DateParameter()
    source_format = bigquery.SourceFormat.CSV
    write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    skip_leading_rows = 1

    def requires(self):
        return GetDailyStockData(self.date)

    def output(self):
        dataset = 'stocks'
        table = 'price_daily'

        return bigquery.BigQueryTarget(PROJECT_ID, dataset, table,
                                       client=BQ_CLIENT)

class QueryStockPriceData(luigi.Task):
    query_str = ('SELECT p.ticker AS ticker, '
                 'p.date AS date, '
                 'p.price AS price, '
                 's.name AS name '
                 'FROM [senpai-io:stocks.price_daily] p '
                 'LEFT OUTER JOIN [senpai-io:stocks.symbol] s '
                 'ON p.ticker = s.ticker ')

    def requires(self):
        return LoadRecordsInTable()

    def output(self):
        return luigi.LocalTarget('output/price_history.csv')

    def run(self):
        df = bq_client.query(self.query_str).to_dataframe()
        with self.output().open('w') as out_file:
            df.to_csv(out_file, index=False)


class QueryStockPriceData(bigquery.BigQueryRunQueryTask):
    query = ('SELECT p.ticker AS ticker, '
             'p.date AS date, '
             'p.price AS price, '
             's.name AS name '
             'FROM [senpai-io:stocks.price_daily] p '
             'LEFT OUTER JOIN [senpai-io:stocks.symbol] s '
             'ON p.ticker = s.ticker ')

    def requires(self):
        return LoadRecordsInTable()

    def output(self):
        dataset = 'stocks'
        table = 'price_history'

        return bigquery.BigQueryTarget(PROJECT_ID, dataset, table,
                                       client=BQ_CLIENT)


class GetPriceHistory(bigquery.BigQueryExtractTask):
    def requires(self):
        return QueryStockPriceData()

    def output(self):
        output_path_template = '{}/{}/data/price_history.csv'
        output_path = output_path_template.format(BUCKET_PATH, BUCKET_SUBDIR)
        return gcs.GCSTarget(output_path, client=GCS_CLIENT)
'''

if __name__ == '__main__':
    luigi.run()
