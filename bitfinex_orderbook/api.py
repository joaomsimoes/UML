import requests
import pandas as pd
from utils.db_conn import conn_database
from datetime import datetime
import logging
import time

logging.basicConfig(level=logging.DEBUG)

if __name__ == '__main__':

    while True:
        try:
            r = 'https://api-pub.bitfinex.com/v2/book/tBTCUSD/P2'

            response = requests.get(r, params={'len': 100})
            df = pd.DataFrame(response.json(), columns=['price', 'count', 'amount'])
            df['time'] = datetime.now()
            df.to_sql('order_book', conn_database(), if_exists='append', index=False)

            logging.debug(datetime.now().strftime('%H:%M:%S') + " ; " + "new files saved" + "\n")

            time.sleep(3600)

        except Exception as e:
            logging.exception(datetime.now().strftime('%H:%M:%S') + " ; " + str(e) + "\n")
