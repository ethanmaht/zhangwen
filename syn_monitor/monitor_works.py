from syn_monitor import syn_monitor_run
from syn_monitor import sql_monitor
from emtools import currency_means as cm
from emtools import read_database as rd
import datetime as dt
import pandas as pd


def monitor_order_table_date(write_host, read_host, write_tab, read_tab, date, num):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=write_tab['db'], tab=write_tab['tab'], num=num), dt.datetime.now())
    read_config = syn_monitor_run.pick_conn_host(num, read_host)
    _data = syn_monitor_run.read_data_by_num(
        read_config, read_tab['db'], read_tab['tab'], read_tab['date_col'],
        date, num, sql_monitor.sql_order_num, is_db=1
    )
    _data['id'] = _data.apply(lambda x: cm.user_date_id(x['date_day'], x['tab_num']), axis=1)
    conn = rd.connect_database_host(write_host['host'], write_host['user'], write_host['pw'])
    rd.insert_to_data(_data, conn, write_tab['db'], write_tab['tab'])
    conn.close()


def monitor_user_table_date(write_host, read_host, write_tab, read_tab, date, num):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=write_tab['db'], tab=write_tab['tab'], num=num), dt.datetime.now())
    read_config = syn_monitor_run.pick_conn_host(num, read_host)
    _data = syn_monitor_run.read_data_by_num(
        read_config, read_tab['db'], read_tab['tab'], read_tab['date_col'],
        date, num, sql_monitor.sql_user_num, is_db=1
    )
    _data['id'] = _data.apply(lambda x: cm.user_date_id(x['date_day'], x['tab_num']), axis=1)
    conn = rd.connect_database_host(write_host['host'], write_host['user'], write_host['pw'])
    rd.insert_to_data(_data, conn, write_tab['db'], write_tab['tab'])
    conn.close()


def monitor_syn_tables(write_host, read_host, write_tab, read_tab, date, num):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=write_tab['db'], tab=write_tab['tab'], num=num), dt.datetime.now())
    data_list = []
    conn = rd.connect_database_host(write_host['host'], write_host['user'], write_host['pw'])
    for _tab in read_tab:
        if _tab['date_type'] == 'date':
            _sql = sql_monitor.sql_tab_data_num_by_date
        else:
            _sql = sql_monitor.sql_tab_data_num_by_stamp
        _data = syn_monitor_run.read_data_by_num(
            conn, _tab['db'], _tab['tab'], _tab['date_col'], date, num, _sql
        )
        data_list.append(_data)
    all_data = pd.concat(data_list)
    rd.delete_last_date(conn, write_tab['db'], write_tab['tab'], write_tab['date_col'], date)
    rd.insert_to_data(all_data, conn, write_tab['db'], write_tab['tab'])
    conn.close()


def comparison_tab_admin_book_val(market_host, write_dict, date, *args):
    conn = rd.connect_database_host(market_host['host'], market_host['user'], market_host['pw'])
    left_tab = pd.read_sql(
        sql_monitor.sql_comparison_admin_book_order_lift.format(date=date), conn
    )
    order_tab = pd.read_sql(
        sql_monitor.sql_comparison_admin_book_order_orders.format(date=date), conn
    )
    user_tab = pd.read_sql(
        sql_monitor.sql_comparison_admin_book_order_users.format(date=date), conn
    )
    left_tab = syn_monitor_run.data_comparison(left_tab, order_tab, 'date_day', ['order_times', 'order_users'])
    left_tab = syn_monitor_run.data_comparison(left_tab, user_tab, 'date_day', ['logon'])
    rd.insert_to_data(left_tab, conn, write_dict['db'], write_dict['tab'])
    conn.close()

