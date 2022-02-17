# -*- coding: utf-8 -*- 
# @Time : 2022/2/16 9:34 下午 
# @Author : taonian@nj.iscas.ac.cn 
# @File : util.py

def history_date(start, end):
    month_dict = {}
    day_list = []
    from pandas.tseries.offsets import MonthEnd
    import pandas as pd

    for beg in pd.date_range(start, end, freq='MS'):
        period_start_string = beg.strftime("%Y-%m-%d")
        period_end_string = (beg + MonthEnd(1)).strftime("%Y-%m-%d")
        month_dict[period_start_string] = period_end_string


    for beg in pd.date_range(start, end, freq='D'):
        period_start_string = beg.strftime("%Y-%m-%d")
        day_list.append(period_start_string)

    # for starts, ends in month_dict.items():
    #     days_list = []
    #     for beg in pd.date_range(starts, ends, freq='D'):
    #         days_list.append(beg.strftime("%Y-%m-%d"))
    #     print(days_list)
    #
    #     for i in range(len(days_list) - 1):
    #         start = days_list[i]
    #         end = days_list[i + 1]

    return month_dict, day_list


if __name__ == '__main__':
    history_date('2020-01-01', '2022-02-01')