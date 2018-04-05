import pandas as pd
from pandas.io.json import json_normalize
import requests
import backoff

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

for k,v in r.json().items():
    print(k, v['ohlc']['close']['price'])

price = [(k, v['ohlc']['close']['price']) for k,v in r.json().items()]
df = pd.DataFrame(price, columns=['ticker', 'price'])
