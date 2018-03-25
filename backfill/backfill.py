from os import environ
import pandas as pd
import quandl as qdl

TOKEN = environ['QUANDL_TOKEN']
qdl.ApiConfig.api_key = TOKEN

ticker_df = pd.read_csv('djia_symbols.csv')
ticker_list = ticker_df.symbol.tolist()

df = qdl.get_table('WIKI/PRICES', ticker=ticker_list,
                   qopts={'columns': ['ticker', 'date', 'close']},
                   date={'gte': '2015-01-01', 'lte': '2018-03-23' },
                   paginate=True)
df = df.rename(columns={'close': 'price'})

df.to_csv('data_jan2015_mar2018.csv', index=False)
