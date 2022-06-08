import requests
import pandas as pd
from utils.db_conn import conn_database
from datetime import datetime
import logging
import time

logging.basicConfig(level=logging.DEBUG)


def api(type=None):
    try:
        r = f'https://api-pub.bitfinex.com/v2/book/tBTCUSD/{type}'

        response = requests.get(r, params={'len': 100})
        df = pd.DataFrame(response.json(), columns=['price', 'count', 'amount'])
        df['time'] = datetime.now()
        df['type'] = type
        df.to_sql('orderBook', conn_database(), if_exists='append', index=False)

        logging.debug(datetime.now().strftime('%H:%M:%S') + " ; " + "new files saved" + "\n")

    except Exception as e:
        logging.debug(datetime.now().strftime('%H:%M:%S') + " ; " + str(e) + "\n")


if __name__ == '__main__':
    while True:
        now = datetime.now()
        if (now.minute == 0) and (now.second == 0):
            for value in ['P0', 'P1', 'P2', 'P3', 'P4']:
                print(value)
                time.sleep(1)
