from algorithm import retained
from algorithm import show_tabel
import time
from es_worker import ec_market
from emtools import read_database as rd
from emtools import data_job
from logs import loger


@loger.logging_read
def syn_market_keep_day(s_date=None):
    work = retained.KeepTableDay(list_day=[1, 2, 3, 7, 14, 30])
    if s_date:
        work.s_date = s_date
    work.count_keep_table_day_run()


@loger.logging_read
def syn_market_keep_day_admin(s_date=None):
    work = retained.RunCount('market_read', 'market_keep_day_admin', 'date_day', extend='list')
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        retained.count_keep_table_day_admin_run,
        follow_func=retained.keep_day_admin_count,
        process_num=16
    )


@loger.logging_read
def syn_market_keep_day_admin_new(s_date=None):
    work = retained.RunCount('market_read', 'market_keep_day_admin_test', 'date_day', extend='delete')
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        retained.user_keep_admin_run,
        follow_func=retained.user_keep_admin_count,
        date_sub=31,
        process_num=8
    )


@loger.logging_read
def syn_admin_book_order(s_date=None):
    work = retained.RunCount('market_read', 'order_logon_conversion', 'order_day', extend='delete')
    if s_date:
        work.s_date = s_date
    work.step_run(retained.count_order_logon_conversion)
    time.sleep(3)
    work.direct_run(retained.compress_order_logon_conversion)


@loger.logging_read
def table_show_logon_admin_book_order(s_date=None):
    work = retained.RunCount('market_show', 'logon_admin_book_val', None)
    if s_date:
        work.s_date = s_date
    work.refresh = 1
    work.direct_run(show_tabel.logon_admin_book_val)


@loger.logging_read
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


@loger.logging_read
def syn_market_logon_compress_thirty_day(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='logon_compress_thirty_day', date_col='logon_date', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.retained_logon_compress_thirty_day,
        follow_func=retained.retained_logon_compress_thirty_day_count,
        date_sub=90,
        process_num=8
    )


@loger.logging_read
def syn_market_book_admin_read_situation(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='book_admin_read_situation_30', date_col='start_date', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.chart_book_admin_read_30,
        follow_func=retained.chart_book_admin_read_count_30,
        date_sub=90,
        process_num=8
    )


@loger.logging_read
def syn_new_user_market_book_admin_read_situation(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='new_user_book_admin_read_situation_30',
        date_col='start_date', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.chart_book_admin_read_new_user_30,
        follow_func=retained.chart_book_admin_read_count_30,
        date_sub=90,
        process_num=8
    )


@loger.logging_read
def sound_market_book_count(s_date):
    work = retained.RunCount(
        write_db='sound', write_tab='market_book_count', date_col='logon_day', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.direct_run_kwargs(
        func=ec_market.sound_book_admin_count,
    )


@loger.logging_read
def sound_market_chapter_count(s_date):
    work = retained.RunCount(
        write_db='sound', write_tab='market_chapter_count', date_col='date_day', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.direct_run_kwargs(
        func=ec_market.sound_chapter_admin_count,
    )


@loger.logging_read
def syn_read_sign_order_count(s_date=None):
    rd.syn_date_block_run(
        data_job.read_sign_order_read, s_date, process_num=12,
        write_conn_fig='datamarket', write_db='market_read', write_tab='sign_order_count'
    )


@loger.logging_read
def conversion_funnel_count(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='conversion_funnel_count', date_col='logon_date', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.chart_book_admin_conversion_funnel,
        follow_func=retained.conversion_funnel_count,
        date_sub=90,
        process_num=12
    )


@loger.logging_read
def conversion_funnel_count_all_book(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='conversion_funnel_count_all_book', date_col='logon_date', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.chart_book_admin_conversion_funnel_all_book,
        follow_func=retained.conversion_funnel_count,
        date_sub=90,
        process_num=8
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
        date_sub=15,
        process_num=12
    )


if __name__ == '__main__':
    print('Start work:')
    """ ****** ↓ 自动并部署 ↓ ****** """
    syn_market_keep_day()  # 老留存 -- 不废弃
    syn_admin_book_order()  # 书籍分销
    table_show_logon_admin_book_order('2020-06-01')  # 书籍分销 展示 -> .1h
    syn_market_logon_compress_thirty_day()  # 注册后30日的订阅
    syn_market_book_admin_read_situation()  # 跟读率 -> .3h
    syn_new_user_market_book_admin_read_situation()  # 新用户-图书跟读率 -> .3h
    syn_market_keep_day_admin_new()  # 带渠道和书的留存数据 -> 3h

    conversion_funnel_count()  # 转化漏斗 -> .2h
    conversion_funnel_count_all_book()  # 转化漏斗-所有书 -> .8h

    sound_market_book_count('2020-04-01')  # 有声book数据 -> .1h
    sound_market_chapter_count('2020-04-01')  # 有声chapter数据 -> .1h

    syn_index_data_run()

    """ ****** ↓ discard ↓ ****** """
    # syn_market_keep_day_by_order_consume()  # 新留存 订阅和充值 -- 废弃 210412
    # syn_market_keep_day()  # 老留存 -- 废弃 210412
