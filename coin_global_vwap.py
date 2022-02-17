import time
import warnings
warnings.filterwarnings("ignore")
import json
import psycopg2
import requests
import sys
import pandas as pd
from datetime import datetime
from datetime import timedelta

from pandas.tseries.offsets import MonthEnd

conn = psycopg2.connect(
                        # database="amber_data",
                        database="gd_tradesrecords",
                        user="postgres",
                        password="galaxydigital",
                        host="10.45.64.76",
                        port="5432",)
cur = conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS spot_market_data.coin_global_vwap
         (
         coin   text NOT NULL,
         tm  timestamp NOT NULL,
         time_interval text NOT NULL,
         price  numeric,   
         volume numeric,
         vwap numeric,
         volume_notional numeric,
         create_time timestamp default current_timestamp,
         PRIMARY KEY (coin, tm, time_interval)
         );''')

conn.commit()

def insert_data(coin,start,end,timeInterval, base_url, headers):
    import time
    if len(start)> 10:
        start_timestamp = start
    else:
        start_timestamp = start + 'T00:00:00'
    if end!="":
        end_timestamp = end + 'T00:00:00'

        params = {'startDate': start_timestamp, 'endDate': end_timestamp, 'timeInterval': timeInterval,
                  "timeFormat": "iso"}
    if end == "":
        params = {'startDate': start_timestamp, 'timeInterval': timeInterval, "timeFormat":"iso"}

    try:
        print(params)
        resp = requests.get(url=base_url, params=params, headers=headers)
    except Exception as e:
        print(e)

    values = resp.json()['payload']['data']

    sql_coin_global_vwap_data = """INSERT INTO spot_market_data.coin_global_vwap (coin,tm,time_interval,price,volume,
                    vwap,volume_notional) VALUES (%(coin)s, %(tm)s,%(time_interval)s, %(price)s, %(volume)s, %(vwap)s, %(volume_notional)s)"""
    try:
        interval_dict = {
            'hour': '1h',
            'minute': '1m',
            'day': '1d',
        }
        time_interval = interval_dict.get(timeInterval)
        for value in values:
            # timeArray = time.localtime(value[0] / 1000)
            # otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

            coin_global_vwap_data = {
                "coin":coin.strip(),
                "tm": value['timestamp'].strip(),
                "time_interval": time_interval,
                "price": float(value['price']),
                "volume": float(value['volume']),
                "vwap": float(value['vwap']),
                "volume_notional": round(float(value['volume']) * float(value['vwap']),8),
            }
            try:
                cur.execute(sql_coin_global_vwap_data, coin_global_vwap_data)
            except Exception as e:
                # print(e)
                conn.rollback()
    except Exception as e:
        # print(e)
        pass
    conn.commit()

def get_interval_vol_data(history_or_now, coin, timeInterval):
    import time
    from pandas.tseries.offsets import MonthEnd
    import pandas as pd
    headers = {
        "Accept": "application/json",
        "x-api-key": "UAK0c028c3100dd891e636c471b40b71c09"
    }
    base_url = "https://web3api.io/api/v2/market/spot/vwap/assets/" + coin \
               + "/historical"
    month_dict = {}

    if history_or_now == "history":
        for beg in pd.date_range('2020-01-01', '2022-02-01', freq='MS'):
            period_start_string = beg.strftime("%Y-%m-%d")
            period_end_string = (beg + MonthEnd(1)).strftime("%Y-%m-%d")
            month_dict[period_start_string] = period_end_string

        for starts, ends in month_dict.items():
            days_list = []
            for beg in pd.date_range(starts, ends, freq='D'):
                days_list.append(beg.strftime("%Y-%m-%d"))

            # tmp_1 = days_list[-1]
            # y_m_d = tmp_1.split("-")
            # if y_m_d[1] != '12':
            #     n_y_m_d = y_m_d[0] + "-" + str(int(y_m_d[1]) + 1).zfill(2) + "-" + "01"
            #     days_list.append(n_y_m_d)
            # else:
            #     n_y_m_d = str(int(y_m_d[0]) + 1) + "-" + "01" + "-" + "01"
            #     days_list.append(n_y_m_d)
            # print(days_list)
            for i in range(len(days_list) - 1):
                start = days_list[i]
                end = days_list[i + 1]
                print(start)
                print(end)
                insert_data(coin,start, end,timeInterval, base_url, headers, )

            print(coin + " " + timeInterval + " "+ starts + " to " + ends + " finish ")
        # conn.close()
    if history_or_now == "now":
        interval_dict = {
            'hour': '1h',
            'minute': '1m',
            'day': '1d',
        }
        time_interval = interval_dict.get(timeInterval)
        select_sql = "select tm from spot_market_data.coin_global_vwap where " \
                     "coin=\'{}\' and time_interval =\'{}\' order by tm desc limit 1;".format(coin, time_interval)


        cur.execute(select_sql)
        rows = cur.fetchall()
        starts = rows[0][0].strftime("%Y-%m-%dT%H:%M:%S")
        ends=""
        # ends = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")
        print(starts)
        insert_data(coin,starts, ends,timeInterval, base_url, headers, )

        # conn.close()

    return True

if __name__ == '__main__':

    # Historical data
    history_or_now = 'history'
    gotc_coin_list = ['BTC', 'ETH', 'SOL', 'AAVE', 'ADA', 'ALGO', 'ATOM', 'BAT', 'BCH', 'BNB', 'BSV', 'DASH', 'DOGE', 'DOT',
                 'ENJ', 'EOS', 'ETC',
                 'HBAR', 'KNC', 'KAVA', 'LINK', 'LTC', 'MANA', 'MATIC', 'OXT', 'REP', 'SHIB', 'SUSHI', 'UNI', 'VET',
                 'XLM', 'XRP', 'XMR', 'XTZ', 'ZEC', 'AVAX', 'GRT', 'YFI', 'OMG', 'MKR', 'COMP']
    interval_list = ["minute", "hour", "day"]
    for timeInterval in interval_list:
        for coin in gotc_coin_list:
            coin = str.lower(coin)
            print(coin)
            try:
                get_interval_vol_data(history_or_now,coin,timeInterval)
                print(str(timeInterval) + " " + str(history_or_now) + " " + str(coin) + " is finished")
            except Exception as e:
                print(e)
            time.sleep(0.1)