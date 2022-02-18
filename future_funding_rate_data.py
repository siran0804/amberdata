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

INSERT_HISTORICAL_SQL = '''INSERT INTO future_market_data.funding_rates_data_historical (instrument,exchange,tm,
fundingInterval,fundingRate,timeInterval) VALUES (%(instrument)s, %(exchange)s, %(timestamp)s, %(fundingInterval)s,
%(fundingRate)s,%(timeInterval)s)'''

cur.execute(SELECT_SQL)
rows = cur.fetchall()


def insert_funding_rate_historical_data(instrument, exchange, start, end, timeInterval):
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
    QUERY_STRING["timeInterval"] = timeInterval
    QUERY_STRING["exchange"] = exchange
    print(QUERY_STRING)
    try:
        response = requests.request("GET", url, headers=HEARERS, params=QUERY_STRING)
        print(response)
        data = response.json()
        for item in data["payload"]['data']:
            item["instrument"] = instrument
            item["timeInterval"] = timeInterval
            print(item)
            try:
                cur.execute(INSERT_HISTORICAL_SQL, item)
            except Exception as e:
                print(e)
                conn.rollback()
        conn.commit()
    except Exception as e:
        print(e)


def future_funding_rate_data_run(timeInterval):
    for row in rows:
        monthdict, days_list = history_date(row[2], row[3])
        if timeInterval == "days":
            for key, value in monthdict.items():
                insert_funding_rate_historical_data(row[0], row[1], key, value, 'days')
        if timeInterval == "hours":
            for i in range(len(days_list) - 1):
                start = days_list[i]
                end = days_list[i + 1]
                insert_funding_rate_historical_data(row[0], row[1], start, end, 'hours')


if __name__ == '__main__':
    timeInterval = "days"
    future_funding_rate_data_run(timeInterval)
    conn.close()