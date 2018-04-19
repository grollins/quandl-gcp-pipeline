import pandas as pd
import dask.dataframe as dd

ticker_df = pd.read_csv('../djia_symbols.csv')
ticker_list = ticker_df.symbol.tolist()

# after downloading full dataset directly from Quandl web site
df = dd.read_csv('/home/geoff/Documents/WIKI_PRICES.csv')
df2 = df[df.date >= '2015-01-01']
df2.compute()
df3 = df2[df2.ticker.isin(ticker_list)].compute()
df3['price'] = df3['adj_close']
df3 = df3[['ticker', 'date', 'price']]
df3.to_csv('data_jan2015_mar2018.csv', index=False)
