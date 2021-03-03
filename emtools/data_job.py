from pandas import DataFrame as df
import pymysql
import pandas as pd
from emtools import sql_code
from emtools import currency_means as cm
from emtools import read_database as rd


def db_work(comm):
    cursor = comm.cursor()
    return cursor


def db_select(conn, sql):
    _df = pd.read_sql(sql, conn)
    return _df


def kan_book_day(conn, name):
    # print(sql_code.sql_kan_book_day.format(date='2021-03-01'))
    print(db_select(conn, sql_code.sql_kan_book_day.format(date='2021-03-01')))


def read_data_user_day(conn, size):
    _s, _e = size['start'], size['end'] + 1
    connect = rd.connect_database_vpn('datamarket')
    for _ in range(_s, _e):
        print('======> is work to {num}'.format(num=_))
        user_consume_day = pd.read_sql(sql_code.sql_user_consume_day.format(_num=_, date='2020-11-01'), conn)
        print('======> is work to {num}-{name}'.format(num=_, name='user_consume_day'), user_consume_day.index.size,
              sql_code.sql_user_consume_day.format(_num=_, date='2020-11-01'))
        user_logon_day = pd.read_sql(sql_code.sql_user_logon_day.format(_num=_, date='2018-11-01'), conn)
        print('======> is work to {num}-{name}'.format(num=_, name='user_logon_day'), user_logon_day.index.size,
              sql_code.sql_user_logon_day.format(_num=_, date='2018-11-01'))
        user_sign_day = pd.read_sql(sql_code.sql_user_sign_day.format(_num=_, date='2020-11-01'), conn)
        print('======> is work to {num}-{name}'.format(num=_, name='user_sign_day'), user_sign_day.index.size,
              sql_code.sql_user_sign_day.format(_num=_, date='2020-11-01'))
        user_order_day = pd.read_sql(sql_code.sql_user_order_day.format(_num=_, date='2020-11-01'), conn)
        print('======> is work to {num}-{name}'.format(num=_, name='user_order_day'), user_order_day.index.size)
        mine_df = cm.df_merge(
            [user_logon_day, user_consume_day, user_sign_day, user_order_day],
            on=['user_id', 'date_day'], how='outer', fill_na=0
        )
        print('====== is merge down {num}{name}'.format(num=_, name='mine_df'), mine_df.index.size)
        mine_df['ud_id'] = mine_df.apply(lambda x: cm.user_date_id(x['date_day'], x['user_id']), axis=1)
        print('====== is insert_to_data down {num}{name}'.format(num=_, name='mine_df'), mine_df.index.size)
        mine_df.to_csv(r'D:\work\test_files\1.csv')
        rd.insert_to_data(mine_df, connect, 'user_day')
    connect.close()

