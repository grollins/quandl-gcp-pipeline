import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
plt.style.use('seaborn-deep')

from numpy import percentile
import pandas as pd
import pymc3 as pm
import luigi

from google.cloud import bigquery
BQ_CLIENT = bigquery.Client()


class QueryStockPriceData(luigi.Task):
    query_str = ('SELECT p.ticker AS ticker, '
                 'p.date AS date, '
                 'p.price AS price, '
                 's.name AS name '
                 'FROM `stocks.price_daily` p '
                 'LEFT OUTER JOIN `stocks.symbol` s '
                 'ON p.ticker = s.ticker ')

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget('output/price_history.csv')

    def run(self):
        df = BQ_CLIENT.query(self.query_str).to_dataframe()
        with self.output().open('w') as out_file:
            df.to_csv(out_file, index=False)


class PlotStockPriceData(luigi.Task):
    def requires(self):
        return QueryStockPriceData()

    def output(self):
        return luigi.LocalTarget('output/price_history.pdf')

    def run(self):
        with self.input().open('r') as f:
            df = pd.read_csv(f)

        df.plot()
        plt.savefig(self.output().fn)


if __name__ == '__main__':
    luigi.run()
