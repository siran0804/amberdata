import requests
from config import *
from init_db import cur, conn
from util import *
import sys

BASE_URL = "https://web3api.io/api/v2/market/swaps/funding-rates/"
SELECT_SQL = '''select * from swap_market_data.funding_rates_info where startdate is not null'''

INSERT_LATEST_SQL = '''INSERT INTO swap_market_data.funding_rates_data_latest (instrument,exchange,tm,
fundingInterval,fundingRate,fundingRateDaily) VALUES (%(instrument)s, %(exchange)s, %(timestamp)s, %(fundingInterval)s,
%(fundingRate)s,%(fundingRateDaily)s)'''

INSERT_HISTORICAL_SQL = '''INSERT INTO swap_market_data.funding_rates_data (instrument,exchange,tm,
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


def insert_funding_rate_historical_data(instrument, exchange,start, end, timeInterval):
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


def swap_funding_rate_data_run(timeInterval):
    for row in rows:

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
                # print(start,end)
                insert_funding_rate_historical_data(row[0], row[1],start, end, 'hours')
                print(start + " " + " " + end + " " + row[0] + " HOURS is finished")
        print("2020-01-01 to 2022-02-01 HOURS funding rate data is finished")


if __name__ == '__main__':
    timeInterval_list = ["hours","days"]
    for timeInterval in timeInterval_list:
        swap_funding_rate_data_run(timeInterval)