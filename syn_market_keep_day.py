from algorithm import retained
from algorithm import show_tabel
import time


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
        process_num=32
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
        write_db='market_read', write_tab='keep_day_by_order_consume', date_col='date_day', extend='delete')
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.retained_three_index_by_user,
        follow_func=retained.retained_three_index_by_user_count,
        date_sub=31,
    )


def syn_market_logon_compress_thirty_day(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='logon_compress_thirty_day', date_col='logon_date', extend='delete')
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.retained_logon_compress_thirty_day,
        follow_func=retained.retained_logon_compress_thirty_day_count,
        date_sub=31,
        process_num=12
    )


def syn_market_book_admin_read_situation(s_date=None):
    work = retained.RunCount(
        write_db='market_read', write_tab='book_admin_read_situation', date_col='start_date', extend='delete')
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=retained.chart_book_admin_read,
        follow_func=retained.chart_book_admin_read_count,
        date_sub=31,
        process_num=12
    )


if __name__ == '__main__':
    print('Start work:')
    syn_market_keep_day()
    syn_admin_book_order()
    table_show_logon_admin_book_order('2020-06-01')
    syn_market_keep_day_by_order_consume()
    syn_market_logon_compress_thirty_day()
    syn_market_book_admin_read_situation()
    # syn_market_keep_day_admin()  # 带渠道和书的留存数据
