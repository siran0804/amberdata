import requests
from config import *
from init_db import cur, conn
import sys

BASE_URL = "https://web3api.io/api/v2/market/spot/ohlcv/information"
INSERT_SQL = '''INSERT INTO spot_market_data.ohlcv_information (pair, exchange)
 VALUES (%(pair)s, %(exchange)s) ON CONFLICT (pair, exchange) DO NOTHING'''


def ohlcv_infomation_insert_data():
    try:
        response = requests.request("GET", BASE_URL, headers=HEARERS, params=QUERY_STRING)
    except Exception as e:
        print("response error")
        print(e)
        return -1
    data = response.json()
    if data['status'] != 200:
        print("Spot Market Data Rate Info get data error!")
        return -1
    try:
        for key, value in data['payload'].items():
            print(key)
            for k, v in value.items():
                item = {}
                item["exchange"] = key
                item["pair"] = k
                print(item)
                try:
                    cur.execute(INSERT_SQL, item)
                except Exception as e:
                    print(e)
                    continue
            conn.commit()
    except Exception as e:
        print(e)


    return True

if __name__ == '__main__':
    result_bool = ohlcv_infomation_insert_data()
    if result_bool ==True:
        print("Global Twap Pair Info is completed")