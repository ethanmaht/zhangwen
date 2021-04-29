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


# one_book_locus('datamarket', 'market_read', 'test', 0, date=None, end_date=None, book_id=10067828)
