import requests
from config import *
from init_db import cur, conn
import sys

BASE_URL = "https://web3api.io/api/v2/market/swaps/open-interest/information"
INSERT_SQL = '''INSERT INTO swap_market_data.open_interest_info (instrument, exchange,startdate,enddate)
 VALUES (%(instrument)s, %(exchange)s, %(startDate)s, %(endDate)s)'''


def open_interest_infomation_insert_data():
    try:
        response = requests.request("GET", BASE_URL, headers=HEARERS, params=QUERY_STRING)
    except Exception as e:
        print(e)
        return -1
    data = response.json()
    if data['status'] != 200:
        print("swap Open Interest Info get data error!")
        return -1
    for item in data['payload']:
        if item["startDate"]:
            print(item)
            try:
                cur.execute(INSERT_SQL, item)
            except Exception as e:
                print(e)
                conn.rollback()
            conn.commit()
    return True

if __name__ == '__main__':
    result_bool = open_interest_infomation_insert_data()
    if result_bool == True:
        print("swap Open Interest  Info is completed")
