import pandas as pd
from emtools import sql_code
from emtools import currency_means as cm
from emtools import read_database as rd
import datetime as dt


def read_data_user_day(conn_fig, size, date, process_num):
    _s, _e = size['start'], size['end'] + 1
    tars = [_ for _ in range(_s, _e)]
    if date:
        cm.thread_work(
            one_user_day, conn_fig, 'datamarket', date, tars=tars, process_num=process_num, interval=0.03, step=1
        )
    # cm.thread_tool(_s, _e, 1, one_user_day, conn_fig, 'datamarket')
    else:
        cm.thread_work(one_user_day, conn_fig, 'datamarket', tars=tars, process_num=process_num, interval=0.03, step=1)


def one_user_day(read_conn_fig, write_conn_fig, date, _):
    # if _ != 47:
    #     print('is has do ', _)
    #     return 0
    read_conn = rd.connect_database_host(read_conn_fig)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    db_name = 'happy_seven'
    table_name = 'user_day_{num}'.format(num=_)
    if not date:
        date = rd.read_last_date(write_conn, db_name, table_name, 'date_day')
    print('======> is work to {num} start '.format(num=_), dt.datetime.now())
    user_consume_day = pd.read_sql(
        sql_code.sql_user_consume_day.format(_num=_, date=date), read_conn,
    )
    user_logon_day = pd.read_sql(
        sql_code.sql_user_logon_day.format(_num=_, date=date), read_conn,
    )
    user_sign_day = pd.read_sql(
        sql_code.sql_user_sign_day.format(_num=_, date=date), read_conn
    )
    user_order_day = pd.read_sql(
        sql_code.sql_user_order_day.format(_num=_, date=date), read_conn
    )
    mine_df = cm.df_merge(
        [user_logon_day, user_consume_day, user_sign_day, user_order_day],
        on=['user_id', 'date_day'], how='outer', fill_na=0
    )
    mine_df['ud_id'] = mine_df.apply(lambda x: cm.user_date_id(x['date_day'], x['user_id']), axis=1)
    mine_df['tab_num'] = _
    rd.insert_to_data(mine_df, write_conn, db_name, table_name)
    read_conn.close()
    write_conn.close()
