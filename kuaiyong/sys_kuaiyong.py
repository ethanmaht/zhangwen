import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from emtools import read_database as rd
from emtools import emdate
from algorithm import retained
from kuaiyong import sql_kuaiyong
import pandas as pd
import os
import json
from emtools import currency_means as cm
import numpy as np
from kuaiyong import kuaiyong_work


def sys_origin_data(date_sub=7):
    admin_info_run(sql_kuaiyong.sql_admin_info, config_name='kuaiyong_cps', db_name='kuaiyong', tab_name='admin_info')
    free_chapter_run(
        sql_kuaiyong.sql_book_free_chapter, config_name='kuaiyong_cps', db_name='kuaiyong', tab_name='free_chapter'
    )
    free_chapter_run(
        sql_kuaiyong.sql_book_info, config_name='kuaiyong_cps', db_name='kuaiyong', tab_name='book_info'
    )
    user_order_run(
        sql_kuaiyong.sql_user_order,
        config_name='kuaiyong_order', db_name='kuaiyong', tab_name='order_log', date_sub=date_sub
    )
    user_info_run(
        sql_kuaiyong.sql_user_info,
        config_name='datamarket_out', db_name='kuaiyong', tab_name='user_info', date_sub=date_sub
    )
    recently_read_run(
        sql_kuaiyong.sql_recently_read_info,
        config_name='datamarket_out', db_name='kuaiyong', tab_name='recently_read', date_sub=date_sub
    )

    # consume_log_run(
    #     sql_kuaiyong.sql_consume_log,
    #     config_name='kuaiyong_consume', db_name='kuaiyong', tab_name='consume_log', date_sub=date_sub
    # )


def admin_info_run(sql, config_name, db_name, tab_name):
    conn = rd.connect_database_vpn(config_name)
    data = pd.read_sql(sql, conn)
    market_conn = rd.connect_database_vpn('datamarket_out')
    rd.insert_to_data(data, market_conn, db_name, tab_name)


def free_chapter_run(sql, config_name, db_name, tab_name):
    conn = rd.connect_database_vpn(config_name)
    data = pd.read_sql(sql, conn)
    market_conn = rd.connect_database_vpn('datamarket_out')
    rd.insert_to_data(data, market_conn, db_name, tab_name)


def user_order_run(sql, config_name, db_name, tab_name, date_sub=2):
    s_date = emdate.date_sub_days(date_sub)
    conn = rd.connect_database_vpn(config_name)
    data = pd.read_sql(sql.format(s_date=s_date), conn)
    first_order = pd.read_sql(sql_kuaiyong.sql_user_first_order, conn)
    data['user_id'] = data['user_id'].astype(int)
    first_order['user_id'] = first_order['user_id'].astype(int)

    data = pd.merge(data, first_order, on='user_id', how='left')

    data['book_id'] = data.apply(lambda x: cm.get_data_by_json_col(x['extend'], 'sourceInfo'), axis=1)

    market_conn = rd.connect_database_vpn('datamarket_out')
    rd.insert_to_data(data, market_conn, db_name, tab_name)


def one_consume_log(read_config, db_name, tab_name, date_col, num, sql, s_date=None):
    read_conn_fig = rd.read_db_host()
    read_host_conn_fig = cm.pick_conn_host_by_num(num, read_conn_fig[read_config])
    data = pd.read_sql(sql.format(s_date=s_date, num=num), read_host_conn_fig)
    market_conn = rd.connect_database_vpn('datamarket_out')
    rd.insert_to_data(data, market_conn, db_name, tab_name)


def consume_log_run(sql, config_name, db_name, tab_name, date_sub, s_date=None):
    work = retained.RunCount(
        write_db=db_name, write_tab=tab_name, date_col='', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.host = rd.read_db_config('datamarket_out')
    work.step_run_kwargs(
        func=one_consume_log,
        run_num=256,
        date_sub=date_sub,
        process_num=12,
        sql=sql
    )


def one_user_info(read_config, db_name, tab_name, date_col, num, sql, s_date=None):
    read_host_conn_fig = rd.connect_database_vpn('kuaiyong_user')
    data = pd.read_sql(sql.format(s_date=s_date, num=num), read_host_conn_fig)

    data['isAddDesktop'] = data.apply(lambda x: cm.get_data_by_json_col(x['extend'], 'isAddDesktop'), axis=1)
    data['book_id'] = data.apply(lambda x: cm.get_data_by_json_col(x['extend'], 'bookId'), axis=1)

    market_conn = rd.connect_database_vpn('datamarket_out')
    rd.insert_to_data(data, market_conn, db_name, tab_name)


def user_info_run(sql, config_name, db_name, tab_name, date_sub, s_date=None):
    work = retained.RunCount(
        write_db=db_name, write_tab=tab_name, date_col='',  # extend='delete'
    )
    work.host = rd.read_db_config('datamarket_out')
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=one_user_info,
        run_num=256,
        date_sub=date_sub,
        process_num=12,
        sql=sql
    )


def one_recently_read(read_config, db_name, tab_name, date_col, num, sql, s_date=None):
    read_host_conn_fig = rd.connect_database_vpn('kuaiyong_read')
    data = pd.read_sql(sql.format(s_date=s_date, num=num), read_host_conn_fig)
    market_conn = rd.connect_database_vpn('datamarket_out')
    rd.insert_to_data(data, market_conn, db_name, tab_name)


def recently_read_run(sql, config_name, db_name, tab_name, date_sub, s_date=None):
    work = retained.RunCount(
        write_db=db_name, write_tab=tab_name, date_col='u_date',  extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.host = rd.read_db_config('datamarket_out')
    work.step_run_kwargs(
        func=one_recently_read,
        run_num=256,
        date_sub=date_sub,
        process_num=12,
        sql=sql
    )


if __name__ == '__main__':
    sys_origin_data(7)
