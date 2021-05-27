from syn_monitor import syn_monitor_run
from syn_monitor import monitor_works
from logs import loger


@loger.logging_read
def order_info_monitor():
    work = syn_monitor_run.SynMonitor(
        {'db': 'syn_monitor', 'tab': 'monitor_orders_syn', 'date_col': 'date_day'},
        {'db': 'cps_user', 'tab': 'orders', 'date_col': 'createtime'}
    )
    work.s_date = '2021-01-01'
    work.syn_step_run(monitor_works.monitor_order_table_date, process_num=32)


@loger.logging_read
def logon_info_monitor():
    work = syn_monitor_run.SynMonitor(
        {'db': 'syn_monitor', 'tab': 'monitor_user_syn', 'date_col': 'date_day'},
        {'db': 'cps_user', 'tab': 'user', 'date_col': 'createtime'}
    )
    # work.s_date = '2020-09-01'
    work.syn_step_run(monitor_works.monitor_user_table_date, process_num=32)


@loger.logging_read
def syn_table_day_nums_monitor():
    work = syn_monitor_run.SynMonitor(
        {'db': 'syn_monitor', 'tab': 'monitor_user_syn', 'date_col': 'date_day'},
        [
            {'db': 'happy_seven', 'tab': 'user_day', 'date_col': 'date_day', 'date_type': 'date'},
            {'db': 'orders_log', 'tab': 'orders_log', 'date_col': 'createtime', 'date_type': 'stamp'},
            {'db': 'user_info', 'tab': 'user_info', 'date_col': 'createtime', 'date_type': 'stamp'},
        ]
    )
    # work.s_date = '2020-09-01'
    work.syn_step_run(monitor_works.monitor_syn_tables, process_num=32)


@loger.logging_read
def comparison_admin_book_order():
    work = syn_monitor_run.SynMonitor(
        {'db': 'syn_monitor', 'tab': 'monitor_admin_book_order', 'date_col': 'date_day'},
        []
    )
    work.direct_run_comparison(monitor_works.comparison_tab_admin_book_val)


def one_book_orders():
    work = syn_monitor_run.SynMonitor(
        {'db': 'syn_monitor', 'tab': 'one_book', 'date_col': 'date_day'},
        {'db': 'orders_log', 'tab': 'orders_log', 'date_col': 'createtime', 'date_type': 'stamp'}
    )
    work.s_date = '2021-04-01'
    work.syn_step_run(monitor_works.comparison_by_book_id, process_num=32)


@loger.logging_read
def one_book_sql():
    work = syn_monitor_run.SynMonitor(
        {'db': 'syn_monitor', 'tab': 'one_book_10077522', 'date_col': 'date_day'},
        {'db': 'orders_log', 'tab': 'orders_log', 'date_col': 'createtime', 'date_type': 'stamp'}
    )
    # work.s_date = '2021-04-01'
    work.syn_step_run(monitor_works.comparison_by_one_sql, process_num=16)


def table_last_date():
    work = syn_monitor_run.SynMonitor(
        {'db': 'syn_monitor', 'tab': 'syn_table_update', 'date_col': 'date_day'},
        {'db': '', 'tab': '', 'date_col': '', 'date_type': ''}
    )
    work.s_date = '2019-01-01'
    target_list = [
        {'db_tab': 'sound.es_log', 'date_col': 'time'},
        {'db_tab': 'sound.market_book_count', 'date_col': 'logon_day'},
        {'db_tab': 'sound.market_chapter_count', 'date_col': 'date_day'},
        {'db_tab': 'market_read.new_user_book_admin_read_situation_30_count', 'date_col': 'start_date'},
        {'db_tab': 'market_read.book_admin_read_situation_30_count', 'date_col': 'start_date'},
        {'db_tab': 'market_read.conversion_funnel_count_all_book_count', 'date_col': 'logon_date'},
        {'db_tab': 'market_read.conversion_funnel_count_count', 'date_col': 'logon_date'},
        {'db_tab': 'market_read.index_data', 'date_col': 'date_day'},
        {'db_tab': 'market_read.index_book_consume', 'date_col': 'date_day'},
        {'db_tab': 'market_read.index_data_month', 'date_col': 'date_month'},
        {'db_tab': 'market_read.market_keep_day_admin_test', 'date_col': 'date_day'},
        {'db_tab': 'kuaiyong.chapter_recently_read', 'date_col': 'read_date'},
        {'db_tab': 'kuaiyong.conversion_logon_book', 'date_col': 'create_date'},
        # {'db_tab': '', 'date_col': ''},
    ]
    work.direct_run(monitor_works.syn_last_date, target_list=target_list)


if __name__ == '__main__':
    print('start work:')
    order_info_monitor()
    logon_info_monitor()
    comparison_admin_book_order()
    table_last_date()

    # one_book_orders()
    # one_book_sql()
    # syn_table_day_nums_monitor()
