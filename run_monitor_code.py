from syn_monitor import syn_monitor_run
from syn_monitor import monitor_works


def order_info_monitor():
    work = syn_monitor_run.SynMonitor(
        {'db': 'syn_monitor', 'tab': 'monitor_orders_syn', 'date_col': 'date_day'},
        {'db': 'cps_user', 'tab': 'orders', 'date_col': 'createtime'}
    )
    work.s_date = '2021-03-01'
    work.syn_step_run(monitor_works.monitor_order_table_date, process_num=64)


def logon_info_monitor():
    work = syn_monitor_run.SynMonitor(
        {'db': 'syn_monitor', 'tab': 'monitor_user_syn', 'date_col': 'date_day'},
        {'db': 'cps_user', 'tab': 'user', 'date_col': 'createtime'}
    )
    work.s_date = '2020-09-01'
    work.syn_step_run(monitor_works.monitor_user_table_date, process_num=64)


if __name__ == '__main__':
    print('start work:')
    order_info_monitor()
    # logon_info_monitor()
