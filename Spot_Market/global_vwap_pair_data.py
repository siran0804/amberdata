import time

import requests
from config import *
from init_db import cur, conn
import sys
from util import *

BASE_URL = "https://web3api.io/api/v2/market/spot/vwap/pairs/"
SELECT_SQL = '''select * from spot_market_data.global_vwap_pair_infomation where startdate is not null'''

INSERT_HISTORICAL_SQL = '''INSERT INTO spot_market_data.global_vwap_pair_data (pair,tm,vwap,
price,volume,timeInterval) VALUES (%(pair)s, %(timestamp)s, %(vwap)s, %(price)s,
%(volume)s,%(timeInterval)s) ON CONFLICT (pair,tm,timeInterval) DO NOTHING'''

cur.execute(SELECT_SQL)
rows = cur.fetchall()


def insert_global_vwap_pair_data(pair, start, end, timeinterval):
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
    # print(QUERY_STRING)
    try:
        response = requests.request("GET", url, headers=HEARERS, params=QUERY_STRING)
        # print(response)
        data = response.json()
        print(data)
        if data["payload"]['data']:
            for item in data["payload"]['data']:
                item["timeInterval"] = timeinterval
                # print(item)
                try:
                    cur.execute(INSERT_HISTORICAL_SQL, item)
                except Exception as e:
                    print(e)
                    conn.rollback()
            conn.commit()
    except Exception as e:
        print(e)

tm_sql = '''select tm from spot_market_data.global_vwap_assets_data where pair=\'{}\' order by tm desc limit 1;'''


def global_vwap_pair_data_run(history_or_now, timeinterval):
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
                start_date = row[1]
                end_date = row[2]
                monthdict, days_list = history_date(row[1], row[2])
        for i in range(len(days_list) - 1):
            start = days_list[i]
            end = days_list[i + 1]
            if timeinterval == "minutes":
                insert_global_vwap_pair_data(row[0], start, end, 'minute')
            if timeinterval == "hours":
                insert_global_vwap_pair_data(row[0], start, end, 'hour')
            if timeinterval == "days":
                insert_global_vwap_pair_data(row[0], start, end, 'day')
            print(start + " " + " " + end + " " + row[0] + " " + timeinterval + " is finished")


if __name__ == '__main__':
    history_or_now = "history"
    timeinterval_list = ["minutes", "hours", "days"]
    for timeinterval in timeinterval_list:
        global_vwap_pair_data_run(history_or_now, timeinterval)
    conn.close()