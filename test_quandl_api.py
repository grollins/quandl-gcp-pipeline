from os import environ
from datetime import datetime, timedelta
import pandas as pd
import quandl as qdl

# Quandl
TOKEN = environ['QUANDL_TOKEN']
qdl.ApiConfig.api_key = TOKEN

# Dates
TODAY = datetime.today()
YESTERDAY = TODAY - timedelta(days=1)

ticker_df = pd.read_csv('djia_symbols.csv')
ticker_list = ticker_df.symbol.tolist()

df = qdl.get_table('WIKI/PRICES', ticker=ticker_list,
                   qopts={'columns': ['ticker', 'date', 'close']},
                   date=YESTERDAY.strftime('%Y-%m-%d'), paginate=True)
df = df.rename(columns={'close': 'price'})
