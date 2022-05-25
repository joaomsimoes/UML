import requests
import pandas as pd
from db_conn import conn_database
from datetime import datetime
import time

while True:
    r = 'https://api-pub.bitfinex.com/v2/book/tBTCUSD/P2'

    response = requests.get(r, params={'len': 100})
    df = pd.DataFrame(response.json(), columns=['price', 'count', 'amount'])
    df['time'] = datetime.now()
    df.to_sql('order_book', conn_database(), if_exists='append', index=False)

    time.sleep(3600)
