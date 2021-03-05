# coding=utf8
import pandas as pd
from pandas import DataFrame as df
from emtools import sql_code
from emtools import currency_means as cm
from emtools import read_database as rd
import datetime as dt
from emtools import emdate


def retain_date_day(conn, db_name, table, date):
    date_dict = emdate.date_num_dict(date, 30)
    first_day, last_day = list(date_dict.keys())[0], list(date_dict.keys())[-1]
    one_day = pd.read_sql(
        sql_code.sql_retain_date_day.format(
            db=db_name, tab=table, date=date,
        ),
        conn
    )
    day_30 = pd.read_sql(
        sql_code.sql_retain_date_day_30.format(
            db=db_name, tab=table, s_date=first_day, e_date=last_day,
        ),
        conn
    )
    day_30['user_id'].astype(int)
    day_30['_'] = 1
    day_30 = day_30.pivot_table(
        index='user_id', columns='date_day', values='_', fill_value=0
    ).reset_index()
    day_30['user_id'] = day_30['user_id'].astype(int)
    one_day['user_id'] = one_day['user_id'].astype(int)
    one_day = pd.merge(one_day, day_30, on='user_id', how='left')
    one_day.rename(columns=date_dict, inplace=True)
    one_day.fillna(0, inplace=True)
    one_day.to_csv(r'D:\work\test_files\t.csv')
    return one_day


class KeepTableDay:
    def __init__(self):
        self.host = '172.16.0.248'
        self.table_ = 'user_day'
        self.s_date = '2021-01-19'
        self.e_date = '2021-04-01'

    def count_keep_table_day_run(self):
        date_list = emdate.date_list(self.s_date, self.e_date, format_code='{Y}-{M}-{D}')
        for _day in date_list:
            print('****** Start to run: count_keep_table_day ******')
            cm.thread_tool(0, 511, 32, self.one_day_run, _day)

    def one_day_run(self, date, num):
        print('======> is run to date:', date, ' num:', num)
        conn = rd.connect_database_host(self.host, 'root', 'Qiyue@123')
        one_day_dict = {'date_day': date, 'tab_num': num}
        table_name = self.table_ + '_' + str(num)
        data_one = retain_date_day(conn, 'happy_seven', table_name, date)
        one_day_dict.update(count_keep_table_day_logon(data_one))
        one_day_dict.update(count_keep_table_day_active(data_one))
        one_day_dict.update(count_keep_table_day_order(data_one))
        one_day_dict = df(one_day_dict, index=[num])
        one_day_dict.fillna(0, inplace=True)
        one_day_dict['ud_id'] = one_day_dict.apply(lambda x: cm.user_date_id(x['date_day'], x['tab_num']), axis=1)
        rd.insert_to_data(one_day_dict, conn, 'market_read', 'keep_table_day')
        conn.close()


def count_keep_table_day_logon(_data):
    _data['logon'] = _data['logon'].astype(int)
    _logon = _data.loc[_data['logon'] > 0]
    all_logon = sum(_logon['logon'])
    logon_2 = sum(_logon['2'])
    logon_3 = sum(_logon['3'])
    logon_7 = sum(_logon['7'])
    logon_14 = sum(_logon['14'])
    logon_30 = sum(_logon['30'])
    return {
        'all_logon': all_logon,
        'logon_2': logon_2,
        'logon_3': logon_3,
        'logon_7': logon_7,
        'logon_14': logon_14,
        'logon_30': logon_30,
    }


def count_keep_table_day_active(_data):
    _active = _data
    all_active = _active.index.size
    active_2 = sum(_active['2'])
    active_3 = sum(_active['3'])
    active_7 = sum(_active['7'])
    active_14 = sum(_active['14'])
    active_30 = sum(_active['30'])
    return {
        'all_active': all_active,
        'active_2': active_2,
        'active_3': active_3,
        'active_7': active_7,
        'active_14': active_14,
        'active_30': active_30,
    }


def count_keep_table_day_order(_data):
    _data['order_success'] = _data['order_success'].astype(int)
    _order = _data.loc[_data['order_success'] > 0]
    all_order = _order.index.size
    order_2 = sum(_order['2'])
    order_3 = sum(_order['3'])
    order_7 = sum(_order['7'])
    order_14 = sum(_order['14'])
    order_30 = sum(_order['30'])
    return {
        'all_order': all_order,
        'order_2': order_2,
        'order_3': order_3,
        'order_7': order_7,
        'order_14': order_14,
        'order_30': order_30,
    }
