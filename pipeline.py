from os import environ
from datetime import datetime, timedelta
import logging

import google.auth
import pandas as pd
import quandl as qdl

import luigi
from luigi.contrib.gcs import GCSClient, GCSTarget


# Google Cloud
CREDENTIALS, _ = google.auth.default()
GCS_CLIENT = GCSClient(CREDENTIALS)
BUCKET_PATH = 'gs://senpai-io.appspot.com/quandl-stage'

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
        output_path_template = '{}/data/{date:%Y-%m-%d}.csv'
        output_path = output_path_template.format(BUCKET_PATH, date=self.date)
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
        output_path_template = '{}/report/{date:%Y-%m-%d}.csv'
        output_path = output_path_template.format(BUCKET_PATH, date=self.date)
        return GCSTarget(output_path, client=GCS_CLIENT)

    def run(self):
        with self.input().open('r') as in_file:
            df = pd.read_csv(in_file)
        with self.output().open('w') as out_file:
            out_file.write(str(df['price'].mean()))


if __name__ == '__main__':
    luigi.run()

