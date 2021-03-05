import pandas as pd
from emtools import sql_code
from emtools import currency_means as cm
from emtools import read_database as rd
import datetime as dt


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
    cm.thread_tool(_s, _e, 3, one_user_day, conn, connect)
    # for _ in range(_s, _e):
    #     if _ < 310:
    #         print('is has do ', _)
    #         continue
    #     print('======> is work to {num} start '.format(num=_), dt.datetime.now())
    #     user_consume_day = pd.read_sql(
    #         sql_code.sql_user_consume_day.format(_num=_, date='2020-12-01'), conn,
    #     )
    #     print('======> is work to {num}-{name}'.format(num=_, name='user_consume_day'), user_consume_day.index.size,)
    #     user_logon_day = pd.read_sql(
    #         sql_code.sql_user_logon_day.format(_num=_, date='2020-11-01'), conn,
    #     )
    #     print('======> is work to {num}-{name}'.format(num=_, name='user_logon_day'), user_logon_day.index.size,)
    #     user_sign_day = pd.read_sql(
    #         sql_code.sql_user_sign_day.format(_num=_, date='2020-12-01'), conn
    #     )
    #     print('======> is work to {num}-{name}'.format(num=_, name='user_sign_day'), user_sign_day.index.size,)
    #     user_order_day = pd.read_sql(
    #         sql_code.sql_user_order_day.format(_num=_, date='2020-12-01'), conn
    #     )
    #     print('======> is work to {num}-{name}'.format(num=_, name='user_order_day'), user_order_day.index.size,)
    #     mine_df = cm.df_merge(
    #         [user_logon_day, user_consume_day, user_sign_day, user_order_day],
    #         on=['user_id', 'date_day'], how='outer', fill_na=0
    #     )
    #     print('======> is merge down {num}-{name}'.format(num=_, name='mine_df'), mine_df.index.size)
    #     mine_df['ud_id'] = mine_df.apply(lambda x: cm.user_date_id(x['date_day'], x['user_id']), axis=1)
    #     print('======> is insert_to_data down {num}-{name}'.format(num=_, name='mine_df'), mine_df.index.size)
    #     mine_df['tab_num'] = _
    #     rd.insert_to_data(mine_df, connect, 'user_day')
    connect.close()


def one_user_day(conn, _, connect):
    # if _ < 310:
    #     print('is has do ', _)
    #     return 0
    print('======> is work to {num} start '.format(num=_), dt.datetime.now())
    print(sql_code.sql_user_consume_day.format(_num=_, date='2020-12-01'))
    user_consume_day = rd.read_from_sql(
        sql_code.sql_user_consume_day.format(_num=_, date='2020-12-01'), conn,
    )
    print('======> is work to {num}-{name}'.format(num=_, name='user_consume_day'), user_consume_day.index.size, )
    user_logon_day = rd.read_from_sql(
        sql_code.sql_user_logon_day.format(_num=_, date='2020-11-01'), conn,
    )
    print('======> is work to {num}-{name}'.format(num=_, name='user_logon_day'), user_logon_day.index.size, )
    user_sign_day = rd.read_from_sql(
        sql_code.sql_user_sign_day.format(_num=_, date='2020-12-01'), conn
    )
    print('======> is work to {num}-{name}'.format(num=_, name='user_sign_day'), user_sign_day.index.size, )
    user_order_day = rd.read_from_sql(
        sql_code.sql_user_order_day.format(_num=_, date='2020-12-01'), conn
    )
    print('======> is work to {num}-{name}'.format(num=_, name='user_order_day'), user_order_day.index.size, )
    mine_df = cm.df_merge(
        [user_logon_day, user_consume_day, user_sign_day, user_order_day],
        on=['user_id', 'date_day'], how='outer', fill_na=0
    )
    print('======> is merge down {num}-{name}'.format(num=_, name='mine_df'), mine_df.index.size)
    mine_df['ud_id'] = mine_df.apply(lambda x: cm.user_date_id(x['date_day'], x['user_id']), axis=1)
    print('======> is insert_to_data down {num}-{name}'.format(num=_, name='mine_df'), mine_df.index.size)
    mine_df['tab_num'] = _
    rd.insert_to_data(mine_df, connect, 'user_day')
