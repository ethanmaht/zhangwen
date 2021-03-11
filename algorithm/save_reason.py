# coding=utf8
import pandas as pd
from pandas import DataFrame as df
from emtools import sql_code
from emtools import currency_means as cm
from emtools import read_database as rd
from emtools import emdate
import datetime as dt


def run_save_reason(_date):
    tar_date_list = emdate.date_list(_date, e_date=dt.datetime.now(), format_code='{Y}-{M}-{D}')
    for _day in tar_date_list:
        print('****** Start to run: {d} - count_keep_table_day ******'.format(d=_day))
        tars = [_ for _ in range(512)]
        cm.thread_work(one_day_run, _day, tars=tars, process_num=32, interval=0.03, step=1)


def one_day_run(date, num):
    print('======> is run run_save_reason to date:', date, ' num:', num)
    conn = rd.connect_database_host('172.16.0.248', 'root', 'Qiyue@123')
    day_pool = [2, 3, 7, 14, 30]
    e_day_list = emdate.date_list(date, num=day_pool, format_code='{Y}-{M}-{D}', direction=1)
    _data = []
    for _day in e_day_list:
        # print(sql_code.analysis_reason_for_save.format(s_day=date, e_day=_day, tab_num=num))
        one_data = pd.read_sql(
            sql_code.analysis_reason_for_save.format(s_day=date, e_day=_day, tab_num=num),
            conn
        )
        one_data['save_day'] = day_pool.pop(0)
        _data.append(one_data)
    one_day_num = pd.concat(_data)
    one_day_num['date_day'] = date
    one_day_num = one_day_num.fillna(0)
    rd.insert_to_data(one_day_num, conn, 'market_read', 'save_reason')


if __name__ == '__main__':
    run_save_reason('2021-01-01')
