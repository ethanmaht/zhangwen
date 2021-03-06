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
import sys
import inspect


def syn_market_keep_day(s_date=None):
    work = retained.KeepTableDay(list_day=[1, 2, 3, 7, 14, 30])
    if s_date:
        work.s_date = s_date
    work.count_keep_table_day_run()


def syn_market_keep_day_admin(s_date=None):
    work = retained.RunCount('market_read', 'market_keep_day_admin', 'date_day', extend='list')
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        retained.count_keep_table_day_admin_run,
        follow_func=retained.keep_day_admin_count,
        process_num=16
    )


def syn_market_keep_day_admin_new(s_date=None):
    work = retained.RunCount('market_read', 'market_keep_day_admin_test', 'date_day', extend='delete')
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        retained.user_keep_admin_run,
        follow_func=retained.user_keep_admin_count,
        date_sub=37,
        process_num=8
    )


def syn_admin_book_order(s_date=None):
    work = retained.RunCount('market_read', 'order_logon_conversion', 'order_day', extend='delete')
    if s_date:
        work.s_date = s_date
    work.step_run(retained.count_order_logon_conversion)
    time.sleep(3)
    work.direct_run(retained.compress_order_logon_conversion)


def table_show_logon_admin_book_order(s_date=None):
    work = retained.RunCount('market_show', 'logon_admin_book_val', None)
    if s_date:
        work.s_date = s_date
    work.refresh = 1
    work.direct_run(show_tabel.logon_admin_book_val)


def syn_market_keep_day_by_order_consume(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='keep_day_by_order_consume', date_col='date_day', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.retained_three_index_by_user,
        follow_func=retained.retained_three_index_by_user_count,
        date_sub=31,
    )


def syn_market_logon_compress_thirty_day(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='logon_compress_thirty_day', date_col='logon_date', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.retained_logon_compress_thirty_day,
        follow_func=retained.retained_logon_compress_thirty_day_count,
        date_sub=60,
        process_num=8
    )


def syn_market_book_admin_read_situation(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='book_admin_read_situation_30', date_col='start_date',  extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.chart_book_admin_read_30,
        follow_func=retained.chart_book_admin_read_count_30,
        date_sub=60,
        process_num=8
    )


def syn_new_user_market_book_admin_read_situation(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='new_user_book_admin_read_situation_30',
        date_col='start_date',  extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.chart_book_admin_read_new_user_30,
        follow_func=retained.chart_book_admin_read_count_30,
        date_sub=90,
        process_num=8
    )


def sound_market_book_count(s_date):
    work = retained.RunCount(
        write_db='sound', write_tab='market_book_count', date_col='logon_day', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.direct_run_kwargs(
        func=ec_market.sound_book_admin_count,
    )


def sound_market_chapter_count(s_date):
    work = retained.RunCount(
        write_db='sound', write_tab='market_chapter_count', date_col='date_day', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.direct_run_kwargs(
        func=ec_market.sound_chapter_admin_count,
    )


def syn_read_sign_order_count(s_date=None):
    rd.syn_date_block_run(
        data_job.read_sign_order_read, s_date, process_num=12,
        write_conn_fig='datamarket', write_db='market_read', write_tab='sign_order_count'
    )


def conversion_funnel_count(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='conversion_funnel_count', date_col='logon_date', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.chart_book_admin_conversion_funnel,
        follow_func=retained.conversion_funnel_count,
        date_sub=60,
        process_num=12
    )


def conversion_funnel_count_all_book(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='conversion_funnel_count_all_book', date_col='logon_date', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.chart_book_admin_conversion_funnel_all_book,
        follow_func=retained.conversion_funnel_count,
        date_sub=60,
        process_num=8
    )


def conversion_message_push_run():
    retained.conversion_message_push(
        read_config='datamarket',
        db_name='market_read',
        tab_name='conversion_custom_message_push'
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
        process_num=1
    )


def syn_index_data_run(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='index_data', date_col='date_day',  # extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.index_data,
        date_sub=3,
        process_num=12
    )


def syn_index_data_month_run(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='index_data_month', date_col='date_month',  # extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.index_data_month,
        date_sub=35,
        process_num=12
    )


def syn_index_book_consume_run(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='index_book_consume', date_col='date_day', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.index_data_book_consume,
        date_sub=2,
        process_num=12
    )


def order_book_date_sub_run(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='order_book_date_sub', date_col='date_day', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.order_book_date_sub,
        date_sub=2,
        process_num=12
    )


def models_keep_data_run(s_date=None):
    work = retained.RunCount(
        write_db='model_keep', write_tab='model_keep', date_col='date_day', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.model_keep_data,
        date_sub=50,
        process_num=8
    )


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


def run_keep_logon():
    config = rd.read_db_config('cps_host')
    read_conn = rd.connect_database_host(config['host'], config['user'], config['pw'])
    referral_info = pd.read_sql('SELECT id referral_id,book_id referral_book from cps.referral', read_conn)
    rd.syn_date_block_run(
        func=retained.keep_logon,
        date='', process_num=4, db_name='heiyan', tab_name='keep_logon', referral_info=referral_info,
    )
    read_conn.close()


if __name__ == '__main__':
    print('Start work:')
    """ ****** ↓ 自动并部署 ↓ ****** """
    syn_market_keep_day()  # 老留存 -- 不废弃
    syn_admin_book_order()  # 书籍分销
    table_show_logon_admin_book_order('2020-06-01')  # 书籍分销 展示 -> .1h
    syn_market_logon_compress_thirty_day('2021-06-01')  # 注册后30日的订阅 -> .3h

    conversion_funnel_count('2021-06-01')  # 转化漏斗 -> .2h
    conversion_funnel_count_all_book('2021-06-01')  # 转化漏斗-所有书 -> .8h

    sound_market_book_count('2020-05-01')  # 有声book数据 -> .1h
    sound_market_chapter_count('2020-05-01')  # 有声chapter数据 -> .1h

    syn_index_data_run()  # 大盘数据 -> .3h
    syn_index_data_month_run()
    syn_index_book_consume_run()

    syn_market_book_admin_read_situation()  # 跟读率 -> .3h
    syn_new_user_market_book_admin_read_situation()  # 新用户-图书跟读率 -> .3h

    conversion_message_push_run()  # 转化漏斗 消息发送 -> .1h

    syn_market_keep_day_admin_new()  # 带渠道和书的留存数据 -> 3h

    models_keep_data_run()  # 留存 按天 -> 3h

    """ ****** ↓ discard ↓ ****** """

    referral_roi_run(s_date='2021-01-01')  # 推广roi
    referral_roi_90_run(s_date='2021-01-01')

    # run_keep_logon()
    # order_book_date_sub_run('2019-01-01')
    # syn_market_keep_day_by_order_consume()  # 新留存 订阅和充值 -- 废弃 210412
    # syn_market_keep_day()  # 老留存 -- 废弃 210412

    # referral_roi_run(s_date='2021-01-01')  # 推广roi
    # referral_roi_90_run(s_date='2021-01-01')
