import psycopg2

conn = psycopg2.connect(database="gd_tradesrecords",
                        user="postgres",
                        password="goumei",
                        host="47.104.27.120",
                        port="5432")
cur = conn.cursor()


global_twap_assets_info_sql = '''CREATE TABLE IF NOT EXISTS spot_market_data.global_twap_assets_infomation
         (asset text NOT NULL,
         startDate timestamp,
         endDate timestamp,
         PRIMARY KEY (asset)
         );'''


global_twap_pair_info_sql = '''CREATE TABLE IF NOT EXISTS spot_market_data.global_twap_pair_infomation
         (pair text NOT NULL,
         startDate timestamp,
         endDate timestamp,
         PRIMARY KEY (pair)
         );'''

global_twap_assets_data_sql = '''CREATE TABLE IF NOT EXISTS spot_market_data.global_twap_assets_data
         (asset text NOT NULL,
         tm timestamp,
         twap text,
         price text,
         volume text,
         timeInterval text,
         PRIMARY KEY (asset, tm, timeInterval)
         );'''

global_twap_pair_data_sql = '''CREATE TABLE IF NOT EXISTS spot_market_data.global_twap_pair_data
         (pair text NOT NULL,
         tm timestamp,
         twap text,
         price text,
         volume text,
         timeInterval text,
         PRIMARY KEY (pair, tm, timeInterval)
         );'''


global_vwap_assets_info_sql = '''CREATE TABLE IF NOT EXISTS spot_market_data.global_vwap_assets_infomation
         (asset text NOT NULL,
         startDate timestamp,
         endDate timestamp,
         PRIMARY KEY (asset)
         );'''

global_vwap_assets_data_sql = '''CREATE TABLE IF NOT EXISTS spot_market_data.global_vwap_assets_data
         (asset text NOT NULL,
         tm timestamp,
         vwap text,
         price text,
         volume text,
         timeInterval text,
         PRIMARY KEY (asset, tm, timeInterval)
         );'''



global_vwap_pair_info_sql = '''CREATE TABLE IF NOT EXISTS spot_market_data.global_vwap_pair_infomation
         (pair text NOT NULL,
         startDate timestamp,
         endDate timestamp,
         PRIMARY KEY (pair)
         );'''


global_vwap_pair_data_sql = '''CREATE TABLE IF NOT EXISTS spot_market_data.global_vwap_pair_data
         (pair text NOT NULL,
         tm timestamp,
         vwap text,
         price text,
         volume text,
         timeInterval text,
         PRIMARY KEY (pair, tm, timeInterval)
         );'''

ohlcv_info_sql = '''CREATE TABLE IF NOT EXISTS spot_market_data.ohlcv_information
         (pair text NOT NULL,
         exchange text NOT NULL,
         PRIMARY KEY (pair, exchange)
         );'''




def init_table():
    cur.execute(global_twap_assets_info_sql)
    cur.execute(global_twap_pair_info_sql)
    cur.execute(global_twap_assets_data_sql)
    cur.execute(global_twap_pair_data_sql)

    cur.execute(global_vwap_assets_info_sql)
    cur.execute(global_vwap_assets_data_sql)
    cur.execute(global_vwap_pair_info_sql)
    cur.execute(global_vwap_pair_data_sql)

    cur.execute(ohlcv_info_sql)
    conn.commit()


if __name__ == '__main__':
    init_table()