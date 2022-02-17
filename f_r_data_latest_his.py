import requests
from config import *
from init_db import cur, conn
from util import *
import sys

BASE_URL = "https://web3api.io/api/v2/market/futures/funding-rates/"
SELECT_SQL = '''select distinct instrument from future_market_data.funding_rates_infomation'''

INSERT_LATEST_SQL = '''INSERT INTO future_market_data.funding_rates_data_latest (instrument,exchange,tm,
fundingInterval,fundingRate,fundingRateDaily) VALUES (%(instrument)s, %(exchange)s, %(timestamp)s, %(fundingInterval)s,
%(fundingRate)s,%(fundingRateDaily)s)'''

INSERT_HISTORICAL_SQL = '''INSERT INTO future_market_data.funding_rates_data_historical (instrument,exchange,tm,
fundingInterval,fundingRate,timeInterval) VALUES (%(instrument)s, %(exchange)s, %(timestamp)s, %(fundingInterval)s,
%(fundingRate)s,%(timeInterval)s)'''

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


def insert_historical_data(start, end, timeInterval):
    for row in rows[:1]:
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
        QUERY_STRING["timeInterval"] = timeInterval
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
            item["timeInterval"] = timeInterval
            print(item)
            try:
                cur.execute(INSERT_HISTORICAL_SQL, item)
            except Exception as e:
                print(e)
                conn.rollback()
    conn.commit()


def run(timeInterval):
    monthdict, days_list = history_date('2020-01-01', '2022-02-01')
    if timeInterval == "days":
        for key, value in monthdict.items():
            insert_historical_data(key, value, 'days')
    if timeInterval == "hours":
        for i in range(len(days_list) - 1):
            start = days_list[i]
            end = days_list[i + 1]
            insert_historical_data(start, end, 'hours')


if __name__ == '__main__':
    timeInterval = "hours"
    run(timeInterval)