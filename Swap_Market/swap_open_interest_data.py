import requests
from config import *
from init_db import cur, conn
import sys
from util import *

BASE_URL = "https://web3api.io/api/v2/market/swaps/open-interest/"
SELECT_SQL = '''select * from swap_market_data.open_interest_info where startdate is not null'''

INSERT_HISTORICAL_SQL = '''INSERT INTO swap_market_data.open_interest_data (instrument,exchange,tm,
value,type,timeInterval) VALUES (%(instrument)s, %(exchange)s, %(timestamp)s, %(value)s,
%(type)s,%(timeInterval)s)'''

cur.execute(SELECT_SQL)
rows = cur.fetchall()


def insert_open_interest_data(instrument, exchange, start, end, timeinterval):
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
    QUERY_STRING["exchange"] = exchange
    QUERY_STRING["timeInterval"] = timeinterval
    # print(QUERY_STRING)
    try:
        response = requests.request("GET", url, headers=HEARERS, params=QUERY_STRING)
        # print(response)
        data = response.json()
        if data["payload"]['data']:
            for item in data["payload"]['data']:
                item["instrument"] = instrument
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


def open_interest_data_run(timeinterval):
    for row in rows:
        if len(row)<2:
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
                insert_open_interest_data(row[0], row[1], start, end, 'minutes')
            if timeinterval == "hours":
                insert_open_interest_data(row[0], row[1], start, end, 'hours')
            if timeinterval == "days":
                insert_open_interest_data(row[0], row[1], start, end, 'days')
            print(start + " " + " " + end + " " + row[0] + " " + timeinterval + " is finished")


if __name__ == '__main__':
    timeinterval_list = ["minutes","hours", "days"]
    for timeinterval in timeinterval_list:
        open_interest_data_run(timeinterval)
    conn.close()