import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
plt.style.use('seaborn-deep')
import seaborn as sns

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
            df = pd.read_csv(f, parse_dates=['date'])

        df.set_index('ticker', inplace=True)
        # df = df.loc[['MSFT', 'AAPL', 'AXP', 'V']]
        df = df.pivot(index='date', columns='name', values='price')
        df.plot(legend=False)
        plt.savefig(self.output().fn)


class ModelStockPriceData(luigi.Task):
    def requires(self):
        return QueryStockPriceData()

    def output(self):
        return luigi.LocalTarget('output/price_model.pdf')

    def run(self):
        with self.input().open('r') as f:
            df = pd.read_csv(f, parse_dates=['date'])

        df.set_index('ticker', inplace=True)
        df = df.loc[['MSFT']]

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


if __name__ == '__main__':
    luigi.run()
