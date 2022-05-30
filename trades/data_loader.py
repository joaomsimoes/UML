import dload
import pandas as pd
import os
import zipfile
from datetime import datetime, timedelta

datetime.today().strftime('%Y-%m')

START_SPOT = "2017-08-01"
START_FUTURES = "2019-09-01"
END = "2022-04-01"
COIN = 'BTCUSDT'


url = []
path = './temp/'


def download_files(type=None):
    print(f'Starting downloading {type}...')
    if type == 'futures':
        date_list = pd.date_range(start=START_FUTURES, end=END)
        for date in date_list:
            file = f"https://data.binance.vision/data/futures/um/monthly/trades/BTCUSDT/BTCUSDT-trades-{date.strftime('%Y-%m')}.zip"
            url.append(file)
    else:
        date_list = pd.date_range(start=START_SPOT, end=END)
        for date in date_list:
            file = f"https://data.binance.vision/data/{type}/monthly/trades/{COIN}/{COIN}-trades-{date.strftime('%Y-%m')}.zip"
            url.append(file)

    dload.save_multi(url_list=url, dir=path)


def create_dataframe(type=None):
    print(f'Unziping {type}')
    columns = ['id', 'price', 'quantity', 'quoteqt', 'timestamp', 'buyer']

    for file in os.listdir(path):
        try:
            if file.endswith('.zip'):
                with zipfile.ZipFile(path + file, 'r') as zip_ref:
                    zip_ref.extractall(path)
                os.remove(path + file)
        except:
            print(f'{file} error')

    print(f'Starting csv...')
    for file in os.listdir(path):
        if file.endswith('.csv'):
            trades = pd.read_csv(path + file, header=None, names=columns, index_col=False)
            trades['timestamp'] = pd.to_datetime(trades.timestamp, unit='ms')
            trades = trades.resample('60min', on='timestamp').agg(
                {'price': 'mean', 'quantity': 'sum'}).reset_index()
            trades.to_csv(f'aggtrades-{type}.csv', mode='a', index=False, header=False)
            os.remove(path + file)


if __name__ == '__main__':
    columns = ['timestamp', 'price', 'quantity']
    df = pd.DataFrame(columns=columns)
    df.to_csv(f'aggtrades-futures.csv', index=False)
    df.to_csv(f'aggtrades-spot.csv', index=False)

    download_files('futures')
    create_dataframe('futures')

    download_files('spot')
    create_dataframe('spot')
