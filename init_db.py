import psycopg2

conn = psycopg2.connect(database="gd_tradesrecords",
                        user="postgres",
                        password="goumei",
                        host="47.104.27.120",
                        port="5432")
cur = conn.cursor()


f_r_info_sql = '''CREATE TABLE IF NOT EXISTS future_market_data.funding_rates_infomation
         (instrument text NOT NULL,
         exchange text NOT NULL,
         startDate timestamp,
         endDate timestamp,
         PRIMARY KEY (instrument, exchange)
         );'''

f_r_data_latest = '''CREATE TABLE IF NOT EXISTS future_market_data.funding_rates_data_latest
         (instrument text NOT NULL,
         exchange text NOT NULL,
         tm timestamp,
         fundingInterval text,
         fundingRate numeric,
         fundingRateDaily numeric,
         PRIMARY KEY (instrument, exchange)
         );'''

f_r_data_historical = '''CREATE TABLE IF NOT EXISTS future_market_data.funding_rates_data_historical
         (instrument text NOT NULL,
         exchange text NOT NULL,
         tm timestamp,
         fundingInterval text,
         fundingRate numeric,
         timeInterval text NOT NULL,
         PRIMARY KEY (instrument, exchange, tm)
         );'''


liquidations_info_sql = '''CREATE TABLE IF NOT EXISTS future_market_data.liquidations_infomation
         (instrument text NOT NULL,
         exchange text NOT NULL,
         startDate timestamp,
         endDate timestamp,
         PRIMARY KEY (instrument, exchange)
         );'''

liquidations_latest = '''CREATE TABLE IF NOT EXISTS future_market_data.liquidations_latest
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

liquidations_historical = '''CREATE TABLE IF NOT EXISTS future_market_data.liquidations_historical
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


def init_table():
    cur.execute(f_r_info_sql)
    cur.execute(f_r_data_latest)
    cur.execute(f_r_data_historical)
    cur.execute(liquidations_info_sql)
    cur.execute(liquidations_latest)
    conn.commit()


if __name__ == '__main__':
    init_table()