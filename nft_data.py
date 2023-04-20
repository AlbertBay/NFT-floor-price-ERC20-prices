import requests
import numpy as np
import pandas as pd
from pandas import json_normalize
import datetime
import time

mnemonic_api_key = "YOUR_MNEMONIC_API_TOKEN"


def get_top_sales_vol(n=20):
    metric = "METRIC_SALES_VOLUME"
    duration = "DURATION_365_DAYS"
    url = f"https://ethereum-rest.api.mnemonichq.com/collections/v1beta2/top/{metric}/{duration}"
    query = {"limit": str(n)}
    HEADER = {"X-API-Key": mnemonic_api_key}
    try:
        response = requests.get(url, headers=HEADER, params=query)
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        raise SystemExit(e)
    if response.status_code != 200:
        print(f'ERROR STATUS_CODE IS {response.status_code}')
        print(response.json())
        exit()
    data = response.json()
    top_collections = json_normalize(data['collections'])
    unique_combinations = top_collections.groupby(['collection.contractAddress', 'collection.name']).apply(
        lambda x: x.name).tolist()
    return unique_combinations


def nft_history(nft_addresses_names=[]):
    unique_combinations = get_top_sales_vol()
    unique_combinations = unique_combinations + nft_addresses_names
    unique_combinations = list(set(unique_combinations))
    NFT_DATA = []
    for _address, _name in unique_combinations:

        contract_address = _address
        duration = "DURATION_365_DAYS"
        group_by_period = "GROUP_BY_PERIOD_1_DAY"
        HEADER = {"X-API-Key": mnemonic_api_key}
        url_coll = f"https://ethereum-rest.api.mnemonichq.com/collections/v1beta2/{contract_address}/prices/{duration}/{group_by_period}"
        try:
            response = requests.get(url_coll, headers=HEADER)
        except requests.exceptions.RequestException as e:  # This is the correct syntax
            raise SystemExit(e)
        if response.status_code != 200:
            print(f'ERROR STATUS_CODE IS {response.status_code}')
            print(response.json())
            exit()

        data = response.json()
        df = json_normalize(data['dataPoints'])
        df['name'] = _name
        df['address'] = _address
        # df.rename(columns={'min' : f'{_name}_floor'}, inplace = True)
        # df.rename(columns={'avg' : f'{_name}_mid'}, inplace = True)
        # df.drop(columns=['max'], inplace = True)
        df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        df['min'] = df['min'].astype(float)
        df['max'] = df['max'].astype(float)
        df['avg'] = df['avg'].astype(float)
        df['timestamp'] = pd.to_datetime(df['timestamp'], format="%Y-%m-%d").dt.date
        NFT_DATA.append(df)
    NFT_DATA = pd.concat(NFT_DATA)
    return NFT_DATA


def get_nft(nft_addresses_names=[]):
    nft_data = nft_history(nft_addresses_names)
    nft_data.to_csv('nft_data.csv')
