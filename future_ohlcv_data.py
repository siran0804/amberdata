import requests
from config import *
from init_db import cur, conn
import sys
from util import *

BASE_URL = "https://web3api.io/api/v2/market/futures/ohlcv/"
SELECT_SQL = '''select * from future_market_data.ohlcv_info where startdate is not null'''

INSERT_HISTORICAL_SQL = '''INSERT INTO future_market_data.ohlcv_historical (instrument,exchange,tm,
open,high,low,close ,volume,timeInterval) VALUES (%(instrument)s, %(exchange)s, %(timestamp)s, %(open)s,
%(high)s,%(low)s,%(close)s,%(volume)s,%(timeInterval)s)'''

cur.execute(SELECT_SQL)
rows = cur.fetchall()


def insert_ohlcv_data(instrument, exchange, start, end, timeinterval):
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
    QUERY_STRING["timeInterval"] = timeinterval
    print(QUERY_STRING)
    try:
        response = requests.request("GET", url, headers=HEARERS, params=QUERY_STRING)
        print(response)
        data = response.json()
        if data["payload"]['data']:
            for item in data["payload"]['data']:
                item["instrument"] = instrument
                item["timeInterval"] = timeinterval
                print(item)
                try:
                    cur.execute(INSERT_HISTORICAL_SQL, item)
                except Exception as e:
                    print(e)
                    conn.rollback()
            conn.commit()
    except Exception as e:
        print(e)


def ohlcv_data_run(timeinterval):
    for row in rows[:2]:
        monthdict, days_list = history_date(row[2], row[3])
        for i in range(len(days_list) - 1):
            start = days_list[i]
            end = days_list[i + 1]
            if timeinterval == "minutes":
                insert_ohlcv_data(row[0], row[1], start, end, 'minutes')
            if timeinterval == "hours":
                insert_ohlcv_data(row[0], row[1], start, end, 'hours')
            if timeinterval == "days":
                insert_ohlcv_data(row[0], row[1], start, end, 'days')


if __name__ == '__main__':
    timeinterval = "days"
    ohlcv_data_run(timeinterval)
    conn.close()
