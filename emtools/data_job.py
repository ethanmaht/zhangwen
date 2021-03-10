import pandas as pd
from emtools import sql_code
from emtools import currency_means as cm
from emtools import read_database as rd
import datetime as dt


# 同步动作数据 << 注册，订阅，充值，签到
def read_data_user_day(conn_fig, size, date, process_num):
    _s, _e = size['start'], size['end'] + 1
    tars = [_ for _ in range(_s, _e)]
    if date:
        cm.thread_work(
            one_user_day, conn_fig, 'datamarket', date, tars=tars, process_num=process_num, interval=0.03, step=1
        )
    else:
        cm.thread_work(
            one_user_day, conn_fig, 'datamarket', None, tars=tars, process_num=process_num, interval=0.03, step=1
        )


def one_user_day(read_conn_fig, write_conn_fig, date, _):
    # if _ != 47:
    #     print('is has do ', _)
    #     return 0
    print('======> is work to read -*- one_user_day -*- ===> num:{num} start '.format(num=_), dt.datetime.now())
    read_conn = rd.connect_database_host(read_conn_fig)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    write_db_name = 'happy_seven'
    write_table_name = 'user_day_{num}'.format(num=_)
    if not date:
        date = rd.read_last_date(write_conn, write_db_name, write_table_name, 'date_day')
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
    rd.insert_to_data(mine_df, write_conn, write_db_name, write_table_name)
    read_conn.close()
    write_conn.close()


# 同步用户和订单数据： 用户 << 首次订单，渠道来源，内容来源
def read_user_and_order(conn_fig, size, date, process_num):
    _s, _e = size['start'], size['end'] + 1
    tars = [_ for _ in range(_s, _e)]
    return 1


def user_and_order(read_conn_fig, write_conn_fig, date, _):
    print('======> is work to read -*- user_and_order -*- ===> num:{num} start '.format(num=_), dt.datetime.now())
    read_conn = rd.connect_database_host(read_conn_fig)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    write_user_db_name = 'user_info'
    write_user_tab_name = 'user_info_{num}'.format(num=_)
    write_order_db_name = 'order_info'
    write_order_tab_name = 'order_info_{num}'.format(num=_)
    if not date:
        user_date = rd.read_last_date(write_conn, write_user_db_name, write_user_tab_name, 'date_day')
        order_date = rd.read_last_date(write_conn, write_order_db_name, write_order_tab_name, 'date_day')
    else:
        user_date, order_date = date, date
    first_order = pd.read_sql(
        sql_code.sql_first_order_time.format(_num=_), read_conn,
    )
    order_info = pd.read_sql(
        sql_code.sql_order_info.format(_num=_, date=date), read_conn,
    )
    order_info = pd.merge(order_info, first_order, on='user_id', how='left')
    user_info = pd.read_sql(
        sql_code.sql_user_info.format(_num=_, date=date), read_conn
    )
    user_referral = pd.read_sql(
        sql_code.sql_user_info.format(_num=_, date=date), read_conn
    )
    rd.insert_to_data(order_info, write_conn, write_order_db_name, write_order_tab_name)
    read_conn.close()
    write_conn.close()
