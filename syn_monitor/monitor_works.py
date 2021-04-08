from syn_monitor import syn_monitor_run
from syn_monitor import sql_monitor
from emtools import currency_means as cm
from emtools import read_database as rd
from emtools import emdate
import datetime as dt
import pandas as pd
from logs import loger


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
    rd.delete_table_data(conn, write_dict['db'], write_dict['tab'])
    e_date = emdate.datetime_format_code(dt.datetime.now())
    left_tab = pd.read_sql(
        sql_monitor.sql_comparison_admin_book_order_lift.format(date=date, e_date=e_date), conn
    )
    order_tab = pd.read_sql(
        sql_monitor.sql_comparison_admin_book_order_orders.format(date=date, e_date=e_date), conn
    )
    user_tab = pd.read_sql(
        sql_monitor.sql_comparison_admin_book_order_users.format(date=date, e_date=e_date), conn
    )
    left_tab = syn_monitor_run.data_comparison(left_tab, order_tab, 'date_day', ['order_times', 'order_users'])
    left_tab = syn_monitor_run.data_comparison(left_tab, user_tab, 'date_day', ['logon'])
    left_tab = left_tab.fillna(0)
    rd.insert_to_data(left_tab, conn, write_dict['db'], write_dict['tab'])
    conn.close()


@loger.logging_read
def comparison_by_book_id(market_host, read_host, write_tab, read_tab, date, num):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=read_tab['db'], tab=read_tab['tab'], num=num), dt.datetime.now())
    conn = rd.connect_database_host(market_host['host'], market_host['user'], market_host['pw'])
    e_date = emdate.datetime_format_code(dt.datetime.now())
    one_book_data = pd.read_sql(
        sql_monitor.sql_comparison_one_book.format(date=date, e_date=e_date, num=num, bookid=10077894), conn
    )
    one_book_data = one_book_data.fillna(0)
    rd.insert_to_data(one_book_data, conn, write_tab['db'], write_tab['tab'])


def comparison_by_one_sql(market_host, read_host, write_tab, read_tab, date, num):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=read_tab['db'], tab=read_tab['tab'], num=num), dt.datetime.now())
    conn = rd.connect_database_host(market_host['host'], market_host['user'], market_host['pw'])
    sql = """
    SELECT a.user_id,a.book_id,a.channel_id,b.is_finish,a.createtime,a.updatetime,book_create,logon_date,
            CAST(if(a.channel_free_chapter_num<>0,a.channel_free_chapter_num,
            if(a.free_chapter_num<>0,a.free_chapter_num,15)) AS SIGNED) free_chapter,
            CAST(b.chapter_num AS SIGNED) last_chapter_id,b.chapter_num,CAST(chapter_id AS SIGNED) chapter_id
    from user_read.user_read_{num} a
    left join market_read.book_info b on b.id = a.book_id
    where a.createtime >= UNIX_TIMESTAMP('2021-04-01') and a.book_id = 10077522
    """
    # sql = """
    # SELECT user_id,referral_book,FROM_UNIXTIME(createtime) date_day
    # from user_read.user_read_{num} where createtime >= UNIX_TIMESTAMP('{date}') and referral_book = 10077522
    # """
    # sql = """
    # SELECT *,date(FROM_UNIXTIME(createtime)) date_day,{num} tab_num
    # from user_read.user_read_{num}
    # where book_id = 10077522
    # """
    e_date = emdate.datetime_format_code(dt.datetime.now())
    one_book_data = pd.read_sql(
        sql.format(date=date, e_date=e_date, num=num, bookid=10077894), conn
    )
    one_book_data = one_book_data.fillna(0)
    rd.insert_to_data(one_book_data, conn, write_tab['db'], write_tab['tab'])


