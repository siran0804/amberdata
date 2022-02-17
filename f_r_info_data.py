import requests
from config import *
from init_db import cur, conn
import sys

BASE_URL = "https://web3api.io/api/v2/market/futures/funding-rates/information"
INSERT_SQL = '''INSERT INTO future_market_data.funding_rates_infomation (instrument, exchange,startdate,enddate)
 VALUES (%(instrument)s, %(exchange)s, %(startDate)s, %(endDate)s)'''


def insert_data():
    try:
        response = requests.request("GET", BASE_URL, headers=HEARERS, params=QUERY_STRING)
    except Exception as e:
        print(e)
        return -1
    data = response.json()
    if data['status'] != 200:
        print("get data error!")
        return -1
    for item in data['payload']:
        print(item)
        try:
            cur.execute(INSERT_SQL, item)
        except Exception as e:
            print(e)
            conn.rollback()
        conn.commit()


if __name__ == '__main__':
    insert_data()