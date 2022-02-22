import time

import requests
from config import *
from init_db import cur, conn
from util import *
import sys

BASE_URL = "https://web3api.io/api/v2/market/futures/funding-rates/"
SELECT_SQL = '''select * from future_market_data.funding_rates_infomation where startdate is not null'''

INSERT_LATEST_SQL = '''INSERT INTO future_market_data.funding_rates_data_latest (instrument,exchange,tm,
fundingInterval,fundingRate,fundingRateDaily) VALUES (%(instrument)s, %(exchange)s, %(timestamp)s, %(fundingInterval)s,
%(fundingRate)s,%(fundingRateDaily)s)'''

INSERT_HISTORICAL_SQL = '''INSERT INTO future_market_data.funding_rates_data (instrument,exchange,tm,
fundingInterval,fundingRate,timeInterval) VALUES (%(instrument)s, %(exchange)s, %(timestamp)s, %(fundingInterval)s,
%(fundingRate)s,%(timeInterval)s)'''

cur.execute(SELECT_SQL)
rows = cur.fetchall()


def insert_funding_rate_historical_data(instrument, exchange, start, end, timeInterval):
    url = BASE_URL + instrument + "/historical"
    # print(url)
    if len(start) > 10:
        start_timestamp = start
    else:
        start_timestamp = start + 'T00:00:00'
    if end != "":
        end_timestamp = end + 'T00:00:00'
    QUERY_STRING["startDate"] = start_timestamp
    QUERY_STRING["endDate"] = end_timestamp
    QUERY_STRING["timeInterval"] = timeInterval
    QUERY_STRING["exchange"] = exchange
    # print(QUERY_STRING)
    try:
        response = requests.request("GET", url, headers=HEARERS, params=QUERY_STRING)
        # print(response)
        data = response.json()
        for item in data["payload"]['data']:
            item["instrument"] = instrument
            item["timeInterval"] = timeInterval
            # print(item)
            try:
                cur.execute(INSERT_HISTORICAL_SQL, item)
            except Exception as e:
                # print(e)
                conn.rollback()
        conn.commit()
    except Exception as e:
        print(e)




tm_sql = '''select tm from future_market_data.funding_rates_data where instrument=\'{}\' and exchange = \'{}\' order by tm desc limit 1;'''


def future_funding_rate_data_run(timeInterval, history_or_now):
    for row in rows:
        if history_or_now == "now":
            time_sql = tm_sql.format(row[0], row[1])
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
            monthdict, days_list = history_date(row[2], row[3])
        if timeInterval == "days":
            for key, value in monthdict.items():
                insert_funding_rate_historical_data(row[0], row[1], key, value, 'days')
                print(key + " " + " " + value + " " + row[0] + " DAYS is finished")
            print("2020-01-01 to 2022-02-01 DAYS funding rate data is finished")
        if timeInterval == "hours":
            for i in range(len(days_list) - 1):
                start = days_list[i]
                end = days_list[i + 1]
                insert_funding_rate_historical_data(row[0], row[1], start, end, 'hours')
                print(start + " " + " " + end + " " + row[0] + " HOURS is finished")
            # print("2020-01-01 to 2022-02-01 HOURS funding rate data is finished")


if __name__ == '__main__':
    timeInterval_list = ["hours","days"]
    history_or_now='now'
    for timeInterval in timeInterval_list:
        future_funding_rate_data_run(timeInterval, history_or_now)
    conn.close()