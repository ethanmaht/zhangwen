from syn_monitor import syn_monitor_run
from syn_monitor import sql_monitor
from emtools import currency_means as cm
from emtools import read_database as rd
import datetime as dt


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
