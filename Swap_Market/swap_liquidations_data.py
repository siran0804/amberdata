import requests
from config import *
from init_db import cur, conn
from util import *
import sys

BASE_URL = "https://web3api.io/api/v2/market/swaps/liquidations/"
SELECT_SQL = '''select * from swap_market_data.liquidations_info where startdate is not null; '''

INSERT_LATEST_SQL = '''INSERT INTO swap_market_data.liquidations_latest (instrument,exchange,tm,
originalQuantity,price,side,status,type,timeInForce) VALUES (%(instrument)s, %(exchange)s, %(timestamp)s, %(originalQuantity)s,
%(price)s,%(side)s,%(status)s,%(type)s,%(timeInForce)s)'''

INSERT_HISTORICAL_SQL = '''INSERT INTO swap_market_data.liquidations_data (instrument,exchange,tm,
originalQuantity,price,side,status,type,timeInForce) VALUES (%(instrument)s, %(exchange)s, %(timestamp)s, %(originalQuantity)s,
%(price)s,%(side)s,%(status)s,%(type)s,%(timeInForce)s)'''

cur.execute(SELECT_SQL)
rows = cur.fetchall()


def insert_liquidations_historical_data(instrument, exchange, start, end):
    url = BASE_URL + instrument + "/historical"

    if len(start) > 10:
        start_timestamp = start
    else:
        start_timestamp = start + 'T00:00:00'
    if end != "":
        end_timestamp = end + 'T00:00:00'
    QUERY_STRING["startDate"] = start_timestamp
    QUERY_STRING["endDate"] = end_timestamp
    QUERY_STRING["exchange"] = exchange
    # print(QUERY_STRING)
    try:
        response = requests.request("GET", url, headers=HEARERS, params=QUERY_STRING)

        data = response.json()
        if data["payload"]['data']:
            for item in data["payload"]['data']:
                item["instrument"] = instrument
                try:
                    cur.execute(INSERT_HISTORICAL_SQL, item)
                except Exception as e:
                    # print(e)
                    conn.rollback()
            conn.commit()
    except Exception as e:
        print(e)


def swap_liquidations_data_run():
    for row in rows:

        start_date = row[2]
        end_date = row[3]
        monthdict, days_list = history_date(start_date, end_date)
        print(monthdict)
        for i in range(len(days_list) - 1):
            start = days_list[i]
            end = days_list[i + 1]
            insert_liquidations_historical_data(row[0], row[1], start, end)
            print(start + " " + " " + end + " " + row[0] + " liquidations data is finished")
    print("2020-01-01 to 2022-02-01 liquidations data is finished")


if __name__ == '__main__':
    swap_liquidations_data_run()

