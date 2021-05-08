import pandas as pd
from emtools import sql_code
from emtools import read_database as rd
import datetime as dt
from emtools import emdate
from emtools import currency_means as cm


def one_book_locus(read_config, db_name, tab_name, num, s_date=None, date_col=None, end_date=None, book_id=None):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=db_name, tab=tab_name, num=num), dt.datetime.now())
    read_conn_fig = rd.read_db_host()
    read_host_conn_fig = cm.pick_conn_host_by_num(num, read_conn_fig['shart_host'])
    read_conn = rd.connect_database_direct(read_host_conn_fig)
    write_conn = rd.connect_database_host(read_config['host'], read_config['user'], read_config['pw'])
    or_data = one_book_locus_read_data(write_conn, read_conn, book_id, num)
    group_df = or_data[['user_id', 'date_day']]
    group_df = group_df.drop_duplicates(['user_id', 'date_day'], keep='first').reset_index()

    sub_df = rd.data_sub(group_df, 'user_id', 'date_day')
    or_data['user_id'] = or_data['user_id'].astype(int)
    sub_df = sub_df.drop_duplicates(['user_id', 'date_day'], keep='first').reset_index()

    _or_data = pd.merge(or_data, sub_df, on=['user_id', 'date_day'], how='left')
    or_data = logon_sub(_or_data, 'logon_date', 'date_day')
    or_data.fillna(0, inplace=True)
    tab_name = tab_name + '_' + str(num)
    rd.delete_table_data(write_conn, db_name, tab_name)
    rd.insert_to_data(or_data, write_conn, db_name, tab_name)


def one_book_locus_read_data(write_conn, conn_online, book_id, num):
    logon = pd.read_sql(
        sql_code.sql_one_book_logon.format(book_id=book_id, num=num), write_conn
    )
    read = pd.read_sql(
        sql_code.sql_one_book_read.format(book_id=book_id, num=num), write_conn
    )
    order = pd.read_sql(
        sql_code.sql_one_book_order.format(book_id=book_id, num=num), write_conn
    )
    consume = pd.read_sql(
        sql_code.sql_one_book_consume.format(book_id=book_id, num=num), conn_online
    )
    user_info = pd.read_sql(
        sql_code.sql_one_book_user_info.format(num=num), write_conn
    )
    log_df = pd.concat([logon, read, order, consume])
    log_df['user_id'] = log_df['user_id'].astype(int)
    log_df = pd.merge(log_df, user_info, on='user_id', how='left')
    return log_df


def logon_sub(_df, logon_col, date_col):
    _df['logon_sub'] = _df.apply(lambda x: emdate.sub_date(x[logon_col], x[date_col]), axis=1)
    return _df


def book_locus_day(
        read_config, db_name, tab_name, num, s_date=None, date_col=None, end_date=None, book_id=None,
        **kwargs
):
    print('======> is start to run {db}.{tab} - {date} {num} ===> start time:'.format(
        db=db_name, tab=tab_name, num=num, date=s_date), dt.datetime.now())
    write_conn = rd.connect_database_host(read_config['host'], read_config['user'], read_config['pw'])
    if not end_date:
        end_date = emdate.date_sub_days(-1, s_date)
    book_locus_book_mid(write_conn, db_name, book_id, num, s_date=s_date, e_date=end_date)


def book_locus_book_mid(conn, db_name, book_id, num, s_date, e_date):
    before_order = pd.read_sql(
        sql_code.sql_order_today_user_before.format(
            book=book_id, num=num, s_date=s_date, e_date=e_date
        ), conn
    )
    before_order['date_day'] = s_date
    rd.insert_to_data(before_order, conn, db_name, 'mid_before_order')

    book_before_users = pd.read_sql(
        sql_code.sql_book_before_users.format(
            book=book_id, num=num, s_date=s_date, e_date=e_date
        ), conn
    )
    book_before_users['date_day'] = s_date
    rd.insert_to_data(book_before_users, conn, db_name, 'mid_book_before_users')

    channel_before_users = pd.read_sql(
        sql_code.sql_channel_before_users.format(
            book=book_id, num=num, s_date=s_date, e_date=e_date
        ), conn
    )
    channel_before_users['date_day'] = s_date
    rd.insert_to_data(channel_before_users, conn, db_name, 'mid_channel_before_users')

    active_sub = pd.read_sql(
        sql_code.sql_active_sub.format(
            book=book_id, num=num, s_date=s_date, e_date=e_date
        ), conn
    )
    active_sub['date_day'] = s_date
    rd.insert_to_data(active_sub, conn, db_name, 'mid_active_sub')

    _logon_sub = pd.read_sql(
        sql_code.sql_logon_sub.format(
            book=book_id, num=num, s_date=s_date, e_date=e_date
        ), conn
    )
    _logon_sub['date_day'] = s_date
    rd.insert_to_data(_logon_sub, conn, db_name, 'mid_logon_sub')

    result_order = pd.read_sql(
        sql_code.sql_result_order.format(
            book=book_id, num=num, s_date=s_date, e_date=e_date
        ), conn
    )
    result_order['date_day'] = s_date
    rd.insert_to_data(result_order, conn, db_name, 'mid_result_order')


def delete_book_locus_book_mid(host, write_db, write_tab, date_type_name, date, tab_list, **kwargs):
    conn = rd.connect_database_host(host['host'], host['user'], host['pw'])
    for _tab in tab_list:
        rd.delete_table_data(conn, write_db, _tab)


def compute_modal_one_day_data(host, write_db, write_tab, date_type_name, date, **kwargs):
    conn = rd.connect_database_host(host['host'], host['user'], host['pw'])
    result_order = pd.read_sql(sql_code.sql_mid_result_order, conn)

    active_sub = pd.read_sql(sql_code.sql_mid_active_sub, conn)
    active_sub = cm.val_to_df_one_lat(active_sub, 'channel_id', 'active_sub')

    book_before_users = pd.read_sql(sql_code.sql_mid_book_before_users, conn)
    channel_before_users = pd.read_sql(sql_code.sql_mid_channel_before_users, conn)

    _logon_sub = pd.read_sql(sql_code.sql_mid_logon_sub, conn)
    _logon_sub = cm.val_to_df_one_lat(_logon_sub, 'channel_id', 'logon_sub')

    before_order_book = pd.read_sql(sql_code.sql_mid_before_order_book, conn)
    before_order_book = cm.val_to_df_one_lat(before_order_book, 'channel_id', 'book_before')

    before_order_money = pd.read_sql(sql_code.sql_mid_before_order_money, conn)
    before_order_money = cm.val_to_df_one_lat(before_order_money, 'channel_id', 'money_box')

    result_order = pd.merge(result_order, active_sub, on='channel_id', how='left')
    result_order = pd.merge(result_order, book_before_users, on='channel_id', how='left')
    result_order = pd.merge(result_order, channel_before_users, on='channel_id', how='left')
    result_order = pd.merge(result_order, _logon_sub, on='channel_id', how='left')
    result_order = pd.merge(result_order, before_order_book, on='channel_id', how='left')
    result_order = pd.merge(result_order, before_order_money, on='channel_id', how='left')

    result_order['date_day'] = date
    result_order.fillna(0, inplace=True)
    rd.insert_to_data(result_order, conn, write_db, write_tab)
