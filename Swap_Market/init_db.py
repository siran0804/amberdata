import psycopg2

conn = psycopg2.connect(database="amber_data",
                        user="postgres",
                        password="galaxydigital",
                        host="10.45.64.76",
                        port="5432")
cur = conn.cursor()


funding_rate_info_table_init_sql = '''CREATE TABLE IF NOT EXISTS swap_market_data.funding_rates_info
         (instrument text NOT NULL,
         exchange text NOT NULL,
         startDate timestamp,
         endDate timestamp,
         PRIMARY KEY (instrument, exchange)
         );'''

funding_rate_data_table_init_sql = '''CREATE TABLE IF NOT EXISTS swap_market_data.funding_rates_data
         (instrument text NOT NULL,
         exchange text NOT NULL,
         tm timestamp,
         fundingInterval text,
         fundingRate numeric,
         timeInterval text NOT NULL,
         PRIMARY KEY (instrument, exchange, tm, timeInterval)
         );'''


liquidations_info_table_init_sql = '''CREATE TABLE IF NOT EXISTS swap_market_data.liquidations_info
         (instrument text NOT NULL,
         exchange text NOT NULL,
         startDate timestamp,
         endDate timestamp,
         PRIMARY KEY (instrument, exchange)
         );'''

liquidations_data_table_init_sql = '''CREATE TABLE IF NOT EXISTS swap_market_data.liquidations_data
         (instrument text NOT NULL,
         exchange text NOT NULL,
         tm timestamp,
         originalQuantity numeric,
         price numeric,
         side text NOT NULL,
         status text NOT NULL,
         type text NOT NULL,
         timeInForce text NOT NULL,
         PRIMARY KEY (instrument, exchange, tm)
         );'''

long_short_ration_info_table_init_sql = '''CREATE TABLE IF NOT EXISTS swap_market_data.long_short_ratio_info
         (instrument text NOT NULL,
         exchange text NOT NULL,
         startDate timestamp,
         endDate timestamp,
         PRIMARY KEY (instrument, exchange)
         );'''

long_short_ration_data_table_init_sql = '''CREATE TABLE IF NOT EXISTS swap_market_data.long_short_ration_data
         (instrument text NOT NULL,
         exchange text NOT NULL,
         tm timestamp,
         ratio numeric,
         longAccount numeric,
         shortAccount numeric,
         period numeric,
         timeFrame text NOT NULL,
         PRIMARY KEY (instrument, exchange, tm)
         );'''

ohlcv_info_table_init_sql = '''CREATE TABLE IF NOT EXISTS swap_market_data.ohlcv_info
         (instrument text NOT NULL,
         exchange text NOT NULL,
         startDate timestamp,
         endDate timestamp,
         PRIMARY KEY (instrument, exchange)
         );'''

ohlcv_data_table_init_sql = '''CREATE TABLE IF NOT EXISTS swap_market_data.ohlcv_data
         (instrument text NOT NULL,
         exchange text NOT NULL,
         tm timestamp,
         open numeric,
         high numeric,
         low numeric,
         close numeric,
         volume numeric,
         timeInterval text NOT NULL,
         PRIMARY KEY (instrument, exchange, tm,timeInterval)
         );'''

open_interest_info_table_init_sql = '''CREATE TABLE IF NOT EXISTS swap_market_data.open_interest_info
         (instrument text NOT NULL,
         exchange text NOT NULL,
         startDate timestamp,
         endDate timestamp,
         PRIMARY KEY (instrument, exchange)
         );'''


open_interest_data_table_init_sql = '''CREATE TABLE IF NOT EXISTS swap_market_data.open_interest_data
         (instrument text NOT NULL,
         exchange text NOT NULL,
         tm timestamp,
         value numeric,
         type text,
         timeInterval text NOT NULL,
         PRIMARY KEY (instrument, exchange, tm, timeInterval)
         );'''

def init_table():
    cur.execute(funding_rate_info_table_init_sql)
    cur.execute(funding_rate_data_table_init_sql)
    cur.execute(liquidations_info_table_init_sql)
    cur.execute(liquidations_data_table_init_sql)
    cur.execute(long_short_ration_info_table_init_sql)
    cur.execute(long_short_ration_data_table_init_sql)
    cur.execute(ohlcv_info_table_init_sql)
    cur.execute(ohlcv_data_table_init_sql)
    cur.execute(open_interest_info_table_init_sql)
    cur.execute(open_interest_data_table_init_sql)
    conn.commit()


if __name__ == '__main__':
    init_table()