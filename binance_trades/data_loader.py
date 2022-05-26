import dload
import pandas as pd
import os
import zipfile
from datetime import datetime, timedelta

datetime.today().strftime('%Y-%m-%d')

START = "2022-05-22"
END = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')
COIN = 'BTCUSDT'
TIME = '1h'

date_list = pd.date_range(start=START, end=END)
url = []
path = './temp/'


def download_files(type=None):
    if type == 'futures':
        for date in date_list:
            file = f"https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-{date.strftime('%Y-%m-%d')}.zip"
            print(file)
            url.append(file)
    else:
        for date in date_list:
            file = f"https://data.binance.vision/data/{type}/daily/aggTrades/{COIN}/{COIN}-aggTrades-{date.strftime('%Y-%m-%d')}.zip"
            url.append(file)

    dload.save_multi(url_list=url, dir=path)


def create_dataframe(type=None):
    columns = ['id', 'price', 'quantity', 'firstTrade', 'lastTrade', 'timestamp', 'buyer']

    df = pd.DataFrame(columns=columns)

    for file in os.listdir(path):
        if file.endswith('.zip'):
            with zipfile.ZipFile(path + file, 'r') as zip_ref:
                zip_ref.extractall(path)
            os.remove(path + file)

    for file in os.listdir(path):
        if file.endswith('.csv'):
            csv = pd.read_csv(path + file, header=None, names=columns, index_col=False)
            df = pd.concat([df, csv], ignore_index=True, names=columns)
            os.remove(path + file)

    df['timestamp'] = pd.to_datetime(df.timestamp, unit='ms')
    df.to_csv(f'aggtrades-{type}.csv')


if __name__ == '__main__':
    download_files('futures')
    create_dataframe('futures')

    download_files('spot')
    create_dataframe('spot')
