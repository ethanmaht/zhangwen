from algorithm import retained
from algorithm import show_tabel
import time
from es_worker import ec_market
from emtools import read_database as rd
from emtools import data_job
from emtools import sql_code
from clickhouse_driver import Client
import pandas as pd
from logs import loger


def referral_roi_run(s_date=None):
    work = retained.RunCount(
        write_db='heiyan', write_tab='referral_roi', date_col='logon_day',  # extend='delete'
    )
    if s_date:
        work.s_date = s_date

    config = rd.read_db_config('click_min')
    client = Client(host=config['host'], user=config['user'], password=config['pw'], database=config['db'])
    delete_sql = sql_code.click_sql_delete_table_data.format(
        db='heiyan', tab='referral_roi', col='logon_day', cd='>=', val=s_date
    )

    rd.execute_click_sql(delete_sql, client)
    referral_info = retained.read_referral_info()
    work.step_run_kwargs(
        func=retained.referral_roi,
        date_sub=0,
        process_num=8,
        referral_info=referral_info
    )


def referral_roi_90_run(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='referral_roi', date_col='referral_day', extend='delete'
    )
    if s_date:
        work.s_date = s_date

    read_config = rd.read_db_config('datamarket')
    conn = rd.connect_database_host(read_config['host'], read_config['user'], read_config['pw'])
    book_info = pd.read_sql(sql_code.sql_keep_book_id_name, conn)
    admin_info = pd.read_sql(sql_code.sql_retained_admin_info, conn)
    book_info['book_id'] = book_info['book_id'].astype(str)
    admin_info['channel_id'] = admin_info['channel_id'].astype(str)
    admin_info.rename(columns={'channel_id': 'admin_id'}, inplace=True)
    config = rd.read_db_config('click_min')
    client = Client(host=config['host'], user=config['user'], password=config['pw'], database=config['db'])
    all_data = rd.read_click_sql(sql_code.sql_referral_roi_all, client)
    all_data['referral_id'] = all_data['referral_id'].astype(str)
    work.step_run_kwargs(
        func=retained.referral_roi_show,
        date_sub=0,
        process_num=1,
        admin_info=admin_info,
        book_info=book_info,
        all_data=all_data,
        run_num=91,
        client=client
    )


def syn_shard_user_day_work(s_date=None):
    shard_user_day_work = rd.DataBaseWork()
    if s_date:
        shard_user_day_work.date = s_date
    shard_user_day_work.date_sub = 7
    shard_user_day_work.loop_all_database()


@loger.logging_read
def syn_date_block(s_date=None):
    rd.syn_date_block_run(
        data_job.read_kd_log, s_date, process_num=8,
        write_conn_fig='datamarket', write_db='log_block', write_tab='action_log'
    )


@loger.logging_read
def syn_read_user_recently_read(s_date=None):
    rd.syn_date_block_run(
        data_job.read_user_recently_read, s_date, process_num=8,
        write_conn_fig='datamarket', write_db='user_read', write_tab='user_read'
    )


@loger.logging_read
def syn_read_sign_order_count(s_date=None):
    rd.syn_date_block_run(
        data_job.read_sign_order_read, s_date, process_num=12,
        write_conn_fig='datamarket', write_db='market_read', write_tab='sign_order_count'
    )


@loger.logging_read
def syn_happy_seven_sound_shard_work(s_date=None):
    rd.syn_date_block_run(
        func=data_job.syn_happy_seven_sound_shard, date=s_date, process_num=8,
        write_conn_fig='datamarket_out', write_db='sound',
        write_tab_list=['user', 'orders']
    )
    rd.syn_date_block_run(
        func=data_job.syn_happy_seven_sound_shard_num, date=s_date, process_num=8,
        write_conn_fig='datamarket_out', write_db='sound',
        write_tab_list=['consume', 'sign']
    )
    data_job.syn_happy_seven_sound_cps('datamarket', 'sound')


@loger.logging_read
def syn_portrait_user_order_run(s_date=None):
    data_job.delete_portrait_user_order(
        write_conn_fig='datamarket', write_db='market_read', write_tab='portrait_user_order'
    )
    rd.syn_date_block_run(
        data_job.portrait_user_order_run, s_date, process_num=16,
        write_conn_fig='datamarket', write_db='market_read', write_tab='portrait_user_order'
    )


@loger.logging_read
def syn_portrait_user_order_admin_book_run(s_date=None):
    data_job.delete_portrait_user_order(
        write_conn_fig='datamarket', write_db='market_read', write_tab='portrait_user_order_admin_book'
    )
    rd.syn_date_block_run(
        data_job.portrait_user_order_run_admin_book, s_date, process_num=12,
        write_conn_fig='datamarket', write_db='market_read', write_tab='portrait_user_order_admin_book'
    )
    data_job.portrait_user_order_run_admin_book_count(
        write_conn_fig='datamarket', write_db='market_read', write_tab='portrait_user_order_admin_book'
    )


def syn_user_date_interval_run(s_date=None):
    work = retained.RunCount(
        write_db='user_interval', write_tab='user_date_interval', date_col='date_day', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=data_job.user_date_interval,
        date_sub=30,
        process_num=8
    )


def syn_one_book_read_run(book_id, s_date=None):
    rd.syn_date_block_run(
        data_job.one_book_read, s_date, process_num=12, book_id=book_id,
        write_conn_fig='datamarket', write_db='one_book_read', write_tab='one_book_consume'
    )
