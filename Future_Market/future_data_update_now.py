from future_funding_rate_data import *
from future_ohlcv_data import *
from future_liquidations_data import *
from future_open_interest_data import *
from future_long_short_ratio_data import *
from multiprocessing import Pool


def future_data(form):
    history_or_now = 'now'
    if form == "rate":
        timeInterval_list = ["hours", "days"]
        for timeInterval in timeInterval_list:
            future_funding_rate_data_run(timeInterval, history_or_now)
    if form == "liquidations":
        future_liquidations_data_run(history_or_now)
    if form == "long_short":
        timeframe_list = ["5m", "1h", "1d"]
        for timeframe in timeframe_list:
            future_long_short_ratio_data_run(history_or_now, timeframe)
    if form == "ohlcv":
        timeinterval_list = ["minutes", "hours", "days"]
        for timeinterval in timeinterval_list:
            ohlcv_data_run(timeinterval)
    if form == "open_interest":
        timeinterval_list = ["minutes", "hours", "days"]
        for timeinterval in timeinterval_list:
            open_interest_data_run(timeinterval)


if __name__ == '__main__':
    form_list = ["rate", "liquidations", "long_short", "ohlcv", "open_interest"]
    p = Pool(5)
    for form in form_list:
        p.apply_async(future_data, args=(form,))
    p.close()
    p.join()


