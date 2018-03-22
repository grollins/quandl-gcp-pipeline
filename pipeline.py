from os import environ
from datetime import datetime, timedelta
import logging

import google.auth
from google.cloud import storage

import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
plt.style.use('seaborn-deep')

from numpy import percentile
import pandas as pd
import pymc3 as pm
import quandl as qdl

import luigi
from luigi.contrib.gcs import GCSClient, GCSTarget


# Google Cloud
BUCKET_NAME = 'senpai-io.appspot.com'
BUCKET_PATH = 'gs://{}'.format(BUCKET_NAME)
BUCKET_SUBDIR = 'quandl-stage'
CREDENTIALS, _ = google.auth.default()
GCS_CLIENT = GCSClient(CREDENTIALS)
GCS_BUCKET = storage.Client().get_bucket(BUCKET_NAME)

# Quandl
TOKEN = environ['QUANDL_TOKEN']
qdl.ApiConfig.api_key = TOKEN

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
        return GCSTarget(output_path, client=GCS_CLIENT)

    def run(self):
        ticker_df = pd.read_csv('djia_symbols.csv')
        ticker_list = ticker_df.symbol.tolist()

        df = qdl.get_table('WIKI/PRICES', ticker=ticker_list,
                           qopts={'columns': ['ticker', 'date', 'close']},
                           date=self.date.strftime('%Y-%m-%d'))
        df = df.rename(columns={'close': 'price'})

        with self.output().open('w') as out_file:
            df.to_csv(out_file, index=False)


class GenerateReport(luigi.Task):
    date = luigi.DateParameter(default=YESTERDAY)

    def requires(self):
        return GetDailyStockData(self.date)

    def output(self):
        output_path_template = '{}/{}/report/{date:%Y-%m-%d}.txt'
        output_path = output_path_template.format(BUCKET_PATH, BUCKET_SUBDIR, date=self.date)
        return GCSTarget(output_path, client=GCS_CLIENT)

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

if __name__ == '__main__':
    luigi.run()

