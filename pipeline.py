from os import environ
from datetime import datetime, timedelta
import logging
import requests
import backoff

import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
plt.style.use('seaborn-deep')

from numpy import percentile
import pandas as pd
import pymc3 as pm

import google.auth
from google.cloud import storage, bigquery

import luigi
from luigi.contrib import gcs as luigi_gcs
from luigi.contrib import bigquery as luigi_bigquery
from luigi.contrib import external_program


# Google Cloud
PROJECT_ID = 'senpai-io'
BUCKET_NAME = 'senpai-io.appspot.com'
BUCKET_PATH = 'gs://{}'.format(BUCKET_NAME)
BUCKET_SUBDIR = 'quandl-stage'
CREDENTIALS, _ = google.auth.default()
GCS_CLIENT = luigi_gcs.GCSClient(CREDENTIALS)
GCS_BUCKET = storage.Client().get_bucket(BUCKET_NAME)
# BQ_CLIENT = luigi_bigquery.BigQueryClient(CREDENTIALS)

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
        return luigi_gcs.GCSTarget(output_path, client=GCS_CLIENT)

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
        df = df[['ticker', 'date', 'price']]

        with self.output().open('w') as out_file:
            df.to_csv(out_file, index=False)

        return


class LoadRecordsInBigQuery(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return GetDailyStockData(self.date)

    def output(self):
        output_path_template = 'output/{date:%Y-%m-%d}_bqload.txt'
        output_path = output_path_template.format(date=self.date)
        return luigi.LocalTarget(output_path)

    def run(self):
        client = bigquery.Client()
        dataset_ref = client.dataset('stocks')
        table_ref = dataset_ref.table('price_daily_v2')
        previous_rows = client.get_table(table_ref).num_rows

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.job.SourceFormat.CSV

        # BigQuery API request
        load_job = client.load_table_from_uri(self.input().path, table_ref,
                                              job_config=job_config)

        # Waits for table load to complete
        load_job.result()

        assert load_job.state == 'DONE'
        current_rows = client.get_table(table_ref).num_rows
        assert (current_rows - previous_rows) > 0

        with self.output().open('w') as out_file:
            out_file.write("Row count before load: {}\n".format(previous_rows))
            out_file.write("Row count after load: {}\n".format(current_rows))
            out_file.write("Row count delta: {}\n".format(current_rows - previous_rows))

        return


class QueryStockPriceData(luigi.Task):
    date = luigi.DateParameter()
    query_str = ('SELECT p.ticker AS ticker, '
                 'p.date AS date, '
                 'p.price AS price, '
                 's.name AS name '
                 'FROM `stocks.price_daily_v2` p '
                 'LEFT OUTER JOIN `stocks.symbol` s '
                 'ON p.ticker = s.ticker ')

    def requires(self):
        return LoadRecordsInBigQuery(self.date)

    def output(self):
        output_path_template = 'output/{date:%Y-%m-%d}_price_history.csv'
        output_path = output_path_template.format(date=self.date)
        return luigi.LocalTarget(output_path)

    def run(self):
        client = bigquery.Client()
        df = client.query(self.query_str).to_dataframe()
        with self.output().open('w') as out_file:
            df.to_csv(out_file, index=False)


class GenerateReport(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return QueryStockPriceData(self.date)

    def output(self):
        output_path_template = '{}/{}/report/{date:%Y-%m-%d}.txt'
        output_path = output_path_template.format(BUCKET_PATH, BUCKET_SUBDIR, date=self.date)
        return luigi_gcs.GCSTarget(output_path, client=GCS_CLIENT)

    def run(self):
        with self.input().open('r') as in_file:
            df = pd.read_csv(in_file)

        df.set_index('ticker', inplace=True)
        df = df.loc[['MSFT', 'AAPL', 'AXP', 'V']]
        df = df.pivot(index='date', columns='name', values='price')
        df.plot()

        local_png_path_template = 'output/{date:%Y-%m-%d}.png'
        local_png_path = local_png_path_template.format(date=self.date)
        plt.savefig(local_png_path)

        remote_png_path_template = '{}/report/{date:%Y-%m-%d}.png'
        remote_png_path = remote_png_path_template.format(BUCKET_SUBDIR, date=self.date)
        GCS_BUCKET.blob(remote_png_path).upload_from_filename(filename=local_png_path)

        with self.output().open('w') as out_file:
            out_file.write(str(df['price'].mean()))

        return


class DeployAppEngine(external_program.ExternalProgramTask):
    date = luigi.DateParameter(default=YESTERDAY)

    def requires(self):
        return GenerateReport(self.date)

    def program_args(self):
        return ['./deploy_app_engine.sh']


if __name__ == '__main__':
    luigi.run()
