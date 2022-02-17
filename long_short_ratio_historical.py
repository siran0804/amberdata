import requests
from config import *
from init_db import cur, conn
import sys
from util import *

BASE_URL = "https://web3api.io/api/v2/market/futures/liquidations/"
SELECT_SQL = '''select distinct instrument from future_market_data.liquidations_infomation'''

INSERT_LATEST_SQL = '''INSERT INTO future_market_data.liquidations_latest (instrument,exchange,tm,
originalQuantity,price,side,status,type,timeInForce) VALUES (%(instrument)s, %(exchange)s, %(timestamp)s, %(originalQuantity)s,
%(price)s,%(side)s,%(status)s,%(type)s,%(timeInForce)s)'''

INSERT_HISTORICAL_SQL = '''INSERT INTO future_market_data.liquidations_historical (instrument,exchange,tm,
originalQuantity,price,side,orderId) VALUES (%(instrument)s, %(exchange)s, %(timestamp)s, %(originalQuantity)s,
%(price)s,%(side)s,%(orderId)s)'''

cur.execute(SELECT_SQL)
rows = cur.fetchall()


def insert_latest_data():
    for row in rows:
        url = BASE_URL + row[0] + "/latest"
        print(url)
        try:
            response = requests.request("GET", url, headers=HEARERS, params=QUERY_STRING)
        except Exception as e:
            print(e)
            continue
        data = response.json()
        if data['status'] != 200:
            print(url)
            continue
        for item in data["payload"]:
            item["instrument"] = row
            try:
                cur.execute(INSERT_LATEST_SQL, item)
            except Exception as e:
                print(e)
                conn.rollback()
    conn.commit()


def insert_historical_data(start, end):
    for row in rows[:2]:
        url = BASE_URL + row[0] + "/historical"
        print(url)
        if len(start) > 10:
            start_timestamp = start
        else:
            start_timestamp = start + 'T00:00:00'
        if end != "":
            end_timestamp = end + 'T00:00:00'
        QUERY_STRING["startDate"] = start_timestamp
        QUERY_STRING["endDate"] = end_timestamp
        print(QUERY_STRING)
        try:
            response = requests.request("GET", url, headers=HEARERS, params=QUERY_STRING)
            print(response)
        except Exception as e:
            print(e)
            continue
        data = response.json()
        if data['status'] != 200:
            print(url)
            continue
        for item in data["payload"]['data']:
            item["instrument"] = row[0]
            print(item)
            try:
                cur.execute(INSERT_HISTORICAL_SQL, item)
            except Exception as e:
                print(e)
                conn.rollback()
    conn.commit()


def run():
    monthdict, days_list = history_date('2020-01-01', '2022-02-01')

    for key, value in monthdict.items():
        insert_historical_data(key, value)


if __name__ == '__main__':
    run()