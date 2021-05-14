import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from emtools import read_database as rd
from algorithm import retained
from kuaiyong import sql_kuaiyong
import pandas as pd
import os
import json
from emtools import currency_means as cm
import numpy as np


def read_user_channel_read(read_config, db_name, tab_name, num, date_col, s_date=None, end_date=None, orders_df=None):
    print('is start to run {num}'.format(num=num))
    read_conn_fig = rd.read_db_host()
    user = rd.connect_database_vpn('kuaiyong_user')
    read = rd.connect_database_vpn('kuaiyong_read')
    read_host_conn_fig = cm.pick_conn_host_by_num(num, read_conn_fig['kuaiyong_consume'])
    consume_conn = rd.connect_database_direct(read_host_conn_fig)
    market_conn = rd.connect_database_vpn('datamarket_out')

    user = pd.read_sql(sql_kuaiyong.sql_user_in_channel.format(num=num), user)
    read = pd.read_sql(sql_kuaiyong.sql_recently_read.format(num=num), read)
    consume = pd.read_sql(sql_kuaiyong.sql_consume.format(num=num), consume_conn)

    user['isAddDesktop'] = user.apply(lambda x: is_add_desktop(x['extend']), axis=1)
    user['user_id'] = user['user_id'].astype(str)
    read['user_id'] = read['user_id'].astype(str)
    read['book_id'] = read['book_id'].astype(str)
    # read['chapter_num'] = read['chapter_num'].astype(str)
    consume['user_id'] = consume['user_id'].astype(str)
    consume['book_id'] = consume['book_id'].astype(str)

    # consume['chapter_num'] = consume['chapter_num'].astype(str)
    one_num_data = pd.merge(user, read, on='user_id', how='left')
    one_num_data = pd.merge(one_num_data, consume, on=['user_id', 'book_id'], how='left')
    one_num_data = pd.merge(one_num_data, orders_df, on=['user_id', 'book_id'], how='left')
    # one_num_data.to_excel(r"D:\kuaiyong\{name}_{num}.xlsx".format(name='kuaiyong', num=num), index=0)
    one_num_data.fillna(0, inplace=True)
    rd.insert_to_data(one_num_data, market_conn, db_name, tab_name)


def read_user_channel_order():
    order_conn = rd.connect_database_vpn('kuaiyong_order')
    orders = pd.read_sql(sql_kuaiyong.sql_orders, order_conn)
    orders['book_id'] = orders.apply(lambda x: _order_book(x['extend']), axis=1)
    orders['user_id'] = orders['user_id'].astype(str)
    orders = orders.pivot_table(
        index=['book_id', 'user_id'], values=['s_order', 'orders'], aggfunc=np.sum
    ).reset_index()
    orders['user_id'] = orders['user_id'].astype(str)
    orders['book_id'] = orders['book_id'].astype(str)
    return orders


def write_data_to_db(_path, db_name, tab_name):
    write_conn = rd.connect_database_vpn('datamarket')
    for root, dirs, files in os.walk(_path):
        for _file in files:
            print(_file)
            data = pd.read_excel(_path + '\\' + _file)
            data.fillna('--', inplace=True)
            rd.insert_to_data(data, write_conn, db_name, tab_name)


def syn_user_date_interval_run(s_date=None, **kwargs):
    work = retained.RunCount(
        write_db='market_read', write_tab='kuaiyong_test', date_col='create_date',
        # extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=read_user_channel_read,
        process_num=16,
        run_num=256,
        **kwargs
    )


def is_add_desktop(_str):
    try:
        _json = json.loads(_str)
        return _json['isAddDesktop']
    except:
        return 0


def _order_book(_str):
    try:
        _json = json.loads(_str)
        return _json['sourceInfo']
    except:
        return 0


if __name__ == "__main__":
    print('start run')
    orders = read_user_channel_order()
    syn_user_date_interval_run('1', orders_df=orders)
    # write_data_to_db('D:\kuaiyong', 'market_read', 'kuaiyong_test')
