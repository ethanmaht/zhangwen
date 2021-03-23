from algorithm import retained
from algorithm import show_tabel
import time


def syn_market_keep_day(s_date=None):
    work = retained.KeepTableDay(list_day=[1, 2, 3, 7, 14, 30])
    if s_date:
        work.s_date = s_date
    work.count_keep_table_day_run()


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
    work = retained.RunCount(write_db='market_read', write_tab='', date_col='')
    if s_date:
        work.s_date = s_date
    work.direct_run(retained.retained_three_index_by_user)


if __name__ == '__main__':
    print('Start work:')
    syn_market_keep_day()
    syn_admin_book_order()
    table_show_logon_admin_book_order('2020-06-01')
