from syn_monitor import syn_monitor_run
from syn_monitor import monitor_works


def order_info_monitor():
    work = syn_monitor_run.SynMonitor(
        {'db': 'syn_monitor', 'tab': 'monitor_orders_syn', 'date_col': 'date_day'},
        {'db': 'cps_user', 'tab': 'orders', 'date_col': 'createtime'}
    )
    work.s_date = '2021-01-01'
    work.syn_step_run(monitor_works.monitor_order_table_date, process_num=32)


def logon_info_monitor():
    work = syn_monitor_run.SynMonitor(
        {'db': 'syn_monitor', 'tab': 'monitor_user_syn', 'date_col': 'date_day'},
        {'db': 'cps_user', 'tab': 'user', 'date_col': 'createtime'}
    )
    # work.s_date = '2020-09-01'
    work.syn_step_run(monitor_works.monitor_user_table_date, process_num=32)


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


def one_book_sql():
    work = syn_monitor_run.SynMonitor(
        {'db': 'syn_monitor', 'tab': 'year_admin_order', 'date_col': 'date_day'},
        {'db': 'orders_log', 'tab': 'orders_log', 'date_col': 'createtime', 'date_type': 'stamp'}
    )
    # work.s_date = '2021-04-01'
    work.syn_step_run(monitor_works.comparison_by_one_sql, process_num=64)


if __name__ == '__main__':
    print('start work:')
    order_info_monitor()
    logon_info_monitor()
    comparison_admin_book_order()

    # one_book_orders()
    # one_book_sql()
    # syn_table_day_nums_monitor()
