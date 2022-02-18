import requests
from config import *
from init_db import cur, conn
import sys
from util import *

BASE_URL = "https://web3api.io/api/v2/market/futures/liquidations/"
SELECT_SQL = '''select * from future_market_data.liquidations_infomation where startdate is not null'''

INSERT_LATEST_SQL = '''INSERT INTO future_market_data.liquidations_latest (instrument,exchange,tm,
originalQuantity,price,side,status,type,timeInForce) VALUES (%(instrument)s, %(exchange)s, %(timestamp)s, %(originalQuantity)s,
%(price)s,%(side)s,%(status)s,%(type)s,%(timeInForce)s)'''

INSERT_HISTORICAL_SQL = '''INSERT INTO future_market_data.liquidations_historical (instrument,exchange,tm,
originalQuantity,price,side,status,type,timeInForce) VALUES (%(instrument)s, %(exchange)s, %(timestamp)s, %(originalQuantity)s,
%(price)s,%(side)s,%(status)s,%(type)s,%(timeInForce)s)'''

cur.execute(SELECT_SQL)
rows = cur.fetchall()


def insert_liquidations_historical_data(instrument, exchange, start, end):
    url = BASE_URL + instrument + "/historical"
    print(url)
    if len(start) > 10:
        start_timestamp = start
    else:
        start_timestamp = start + 'T00:00:00'
    if end != "":
        end_timestamp = end + 'T00:00:00'
    QUERY_STRING["startDate"] = start_timestamp
    QUERY_STRING["endDate"] = end_timestamp
    QUERY_STRING["exchange"] = exchange
    print(QUERY_STRING)
    try:
        response = requests.request("GET", url, headers=HEARERS, params=QUERY_STRING)
        print(response)
    except Exception as e:
        print(e)
    data = response.json()
    if data["payload"]['data']:
        for item in data["payload"]['data']:
            item["instrument"] = instrument
            print(item)
            try:
                cur.execute(INSERT_HISTORICAL_SQL, item)
            except Exception as e:
                print(e)
                conn.rollback()
        conn.commit()


def future_liquidations_data_run():
    for row in rows:
        monthdict, days_list = history_date(row[2], row[3])
        print(monthdict)
        for key, value in monthdict.items():
            insert_liquidations_historical_data(row[0], row[1], key, value)


if __name__ == '__main__':
    future_liquidations_data_run()