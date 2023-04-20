import requests
import datetime
import pandas as pd
from pandas import json_normalize

crypto_compare_api = 'YOUR_CRYPTO_COMPARE_API_TOKEN'


def get_top_cap(n):
    url = f'https://min-api.cryptocompare.com/data/top/mktcapfull?limit={n}&tsym=USD&api_key={crypto_compare_api}'
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        raise SystemExit(e)
    if response.status_code != 200:
        print(f'ERROR STATUS_CODE IS {response.status_code}')
        print(response.json())
        exit()
    data = response.json()
    tickers = []
    for i in range(n):
        tickers.append(data['Data'][i]['CoinInfo']['Name'])
    return tickers


def erc20_history(tickers=[]):
    all_tickers = tickers + get_top_cap(20)
    all_tickers = list(set(all_tickers))
    ERC20_DATA = []

    for _ticker in all_tickers:
        url = f'https://min-api.cryptocompare.com/data/v2/histoday?fsym={_ticker}&tsym=USDT&limit=365&api_key={crypto_compare_api}'
        try:
            response = requests.get(url)
        except requests.exceptions.RequestException as e:  # This is the correct syntax
            raise SystemExit(e)
        if response.status_code != 200:
            print(f'ERROR STATUS_CODE IS {response.status_code} FOR TOKEN STATS EXTRACTING')
            print(response.json())
            exit()
        df = json_normalize(response.json()['Data']['Data'])
        df = df[['time', 'close']].rename(columns={'time': 'timestamp', 'close': 'rate'})
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df['ticker'] = _ticker
        ERC20_DATA.append(df)
    ERC20_DATA = pd.concat(ERC20_DATA)[:-1]
    return ERC20_DATA


def get_erc20(token_names=[]):
    erc20_data = erc20_history(token_names)
    erc20_data.to_csv('erc20_data.csv')
