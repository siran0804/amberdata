import requests
from config import *
from init_db import cur, conn
import sys

BASE_URL = "https://web3api.io/api/v2/market/spot/twap/pairs/information"
INSERT_SQL = '''INSERT INTO spot_market_data.global_twap_pair_infomation (pair, startdate,enddate)
 VALUES (%(pair)s, %(startDate)s, %(endDate)s) ON CONFLICT (pair) DO NOTHING'''


def global_twap_pair_infomation_insert_data():
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

    print(len(data['payload']))
    for item in data['payload']:
        try:
            cur.execute(INSERT_SQL, item)
        except Exception as e:
            print(e)
            print(item)
            conn.rollback()
        conn.commit()
    return True

if __name__ == '__main__':
    result_bool = global_twap_pair_infomation_insert_data()
    if result_bool ==True:
        print("Global Twap Pair Info is completed")