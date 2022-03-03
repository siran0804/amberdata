import time

import requests
from config import *
from init_db import cur, conn
import sys
from util import *

BASE_URL = "https://web3api.io/api/v2/market/spot/ohlcv/"
SELECT_SQL = '''select * from spot_market_data.ohlcv_information'''

INSERT_HISTORICAL_SQL = '''INSERT INTO spot_market_data.ohlcv_data (pair,exchange,timeInterval,tm,
open,high,low,close,volume) VALUES (%(pair)s, %(exchange)s, %(timeInterval)s, %(tm)s,
%(open)s,%(high)s,%(low)s,%(close)s,%(volume)s) ON CONFLICT (pair,tm,timeInterval,exchange) DO NOTHING'''

cur.execute(SELECT_SQL)
rows = cur.fetchall()


def insert_ohlcv_data(pair, exchange, start, end, timeinterval):
    url = BASE_URL + pair + "/historical"
    # print(url)
    if len(start) > 10:
        start_timestamp = start
    else:
        start_timestamp = start + 'T00:00:00'
    if end != "":
        end_timestamp = end + 'T00:00:00'
    QUERY_STRING["startDate"] = start_timestamp
    QUERY_STRING["endDate"] = end_timestamp
    QUERY_STRING["timeInterval"] = timeinterval
    QUERY_STRING["exchange"] = exchange
    # print(QUERY_STRING)
    try:
        response = requests.request("GET", url, headers=HEARERS, params=QUERY_STRING)
        # print(response)
        data = response.json()
        print(data)
        if data["payload"]['data']:
            for item in data["payload"]['data'][exchange]:
                temp = {}
                temp["timeInterval"] = timeinterval
                temp["exchange"] = exchange
                temp["pair"] = pair
                temp["tm"] = item[0]
                temp["open"] = item[1]
                temp["high"] = item[2]
                temp["low"] = item[3]
                temp["close"] = item[4]
                temp["volume"] = item[5]
                print(temp)
                try:
                    cur.execute(INSERT_HISTORICAL_SQL, temp)
                except Exception as e:
                    print(e)
                    conn.rollback()
            conn.commit()
    except Exception as e:
        print(e)

tm_sql = '''select tm from spot_market_data.ohlcv_data where pair=\'{}\' order by tm desc limit 1;'''


def ohlcv_data_run(history_or_now, timeinterval):
    for row in rows:
        if history_or_now == "now":
            time_sql = tm_sql.format(row[0])
            cur.execute(time_sql)
            result = cur.fetchone()
            print(result)
            now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            monthdict, days_list = history_date(result[0], now_time)
            days_list[-1] = now_time
            if len(monthdict) == 0:
                start = days_list[0]
                end = days_list[-1]
                monthdict[start] = end
        else:
            if len(row) < 2:
                start_date = '2020-01-01'
                end_date = '2022-02-01'
            else:
                start_date = row[2]
                end_date = row[3]
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
            print(start + " " + " " + end + " " + row[0] + " " + timeinterval + " is finished")


if __name__ == '__main__':
    history_or_now = "history"
    timeinterval_list = ["minutes", "hours", "days"]
    for timeinterval in timeinterval_list:
        ohlcv_data_run(history_or_now, timeinterval)
    conn.close()