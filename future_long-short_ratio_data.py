import requests
from config import *
from init_db import cur, conn
import sys
from util import *

BASE_URL = "https://web3api.io/api/v2/market/futures/long-short-ratio/"
SELECT_SQL = '''select * from future_market_data.long_short_ratio_info where startdate is not null'''

INSERT_HISTORICAL_SQL = '''INSERT INTO future_market_data.long_short_ration_historical (instrument,exchange,tm,
ratio,longAccount,shortAccount,period,timeFrame) VALUES (%(instrument)s, %(exchange)s, %(timestamp)s, %(ratio)s,
%(longAccount)s,%(shortAccount)s,%(period)s,%(timeFrame)s)'''

cur.execute(SELECT_SQL)
rows = cur.fetchall()


def insert_future_long_short_ratio_data(instrument, exchange, start, end, timeframe):
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
        data = response.json()
        if data["payload"]['data']:
            for item in data["payload"]['data']:
                item["instrument"] = instrument
                item["timeFrame"] = timeframe
                print(item)
                try:
                    cur.execute(INSERT_HISTORICAL_SQL, item)
                except Exception as e:
                    print(e)
                    conn.rollback()
            conn.commit()
    except Exception as e:
        print(e)


def future_long_short_ratio_data_run(timeframe):
    for row in rows[:2]:
        monthdict, days_list = history_date(row[2], row[3])
        for i in range(len(days_list) - 1):
            start = days_list[i]
            end = days_list[i + 1]
            if timeframe == "5m":
                insert_future_long_short_ratio_data(row[0], row[1], start, end, '5m')
            if timeframe == "1h":
                insert_future_long_short_ratio_data(row[0], row[1], start, end, '1h')
            if timeframe == "1d":
                insert_future_long_short_ratio_data(row[0], row[1], start, end, '1d')


if __name__ == '__main__':
    timeframe = "1d"
    future_long_short_ratio_data_run(timeframe)
    conn.close()