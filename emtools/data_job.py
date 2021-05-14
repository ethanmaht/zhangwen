import pandas as pd
from emtools import sql_code
from emtools import currency_means as cm
from emtools import read_database as rd
from emtools import emdate
import datetime as dt
from logs import loger
import os
import json
import time


# 同步动作数据 << 注册，订阅，充值，签到
@loger.logging_read
def read_data_user_day(conn_fig, size, date, process_num, date_sub=None):
    _s, _e = size['start'], size['end'] + 1
    tars = [_ for _ in range(_s, _e)]
    if date:
        cm.thread_work(
            one_user_day, conn_fig, 'datamarket', date, process_num,
            tars=tars, process_num=process_num, interval=0.03, step=1
        )
    else:
        cm.thread_work(
            one_user_day, conn_fig, 'datamarket', None, process_num,
            tars=tars, process_num=process_num, interval=0.03, step=1
        )


def one_user_day(read_conn_fig, write_conn_fig, date, date_sub, _):
    print('======> is work to read -*- one_user_day -*- ===> num:{num} start '.format(num=_), dt.datetime.now())
    read_conn = rd.connect_database_host(read_conn_fig)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    write_db_name = 'happy_seven'
    write_table_name = 'user_day_{num}'.format(num=_)
    if not date:
        try:
            date = rd.read_last_date(write_conn, write_db_name, write_table_name, 'date_day')
        except:
            date = '2019-01-01'
    if date_sub:
        date = emdate.date_sub_days(date_sub, date)
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
    rd.insert_to_data(mine_df, write_conn, write_db_name, write_table_name, 'ud_id')
    read_conn.close()
    write_conn.close()


# 同步用户和订单数据： 用户 << 首次订单，渠道来源，内容来源
def read_user_and_order(conn_fig, size, date, process_num, date_sub=None):
    _s, _e = size['start'], size['end'] + 1
    write_conn = rd.connect_database_vpn('datamarket')
    referral_data = pd.read_sql(sql_code.sql_referral_dict, write_conn)
    tars = [_ for _ in range(_s, _e)]
    if date:
        cm.thread_work(
            user_and_order, conn_fig, 'datamarket', date, referral_data, date_sub,
            tars=tars, process_num=process_num, interval=0.03, step=1
        )
    else:
        cm.thread_work(
            user_and_order, conn_fig, 'datamarket', None, referral_data, date_sub,
            tars=tars, process_num=process_num, interval=0.03, step=1
        )
    write_conn.close()


def user_and_order(read_conn_fig, write_conn_fig, date, referral_data, date_sub, _):
    print('======> is work to read -*- user_and_order -*- ===> num:{num} start '.format(num=_), dt.datetime.now())
    read_conn = rd.connect_database_host(read_conn_fig)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    write_user_db_name = 'user_info'
    write_user_tab_name = 'user_info_{num}'.format(num=_)
    write_order_db_name = 'orders_log'
    write_order_tab_name = 'orders_log_{num}'.format(num=_)
    if not date:
        user_date = rd.read_last_date(write_conn, write_user_db_name, write_user_tab_name, 'createtime')
        order_date = rd.read_last_date(write_conn, write_order_db_name, write_order_tab_name, 'updatetime')
    else:
        user_date, order_date = date, date
    if date_sub:
        user_date = emdate.date_sub_days_stamp(date_sub, user_date)
        order_date = emdate.date_sub_days_stamp(date_sub, order_date)
    first_order = pd.read_sql(
        sql_code.sql_first_order_time.format(_num=_), read_conn,
    )

    user_info = pd.read_sql(
        sql_code.sql_user_info.format(_num=_, date=user_date), read_conn
    )
    user_info = pd.merge(user_info, referral_data, left_on='referral_id_permanent', right_on='id', how='left')
    user_info = pd.merge(user_info, first_order, on='user_id', how='left')
    user_info = user_info.fillna(0)
    rd.insert_to_data(user_info, write_conn, write_user_db_name, write_user_tab_name, key_name='user_id')

    _user_info = user_info[['user_id', 'referral_book']]
    order_info = pd.read_sql(
        sql_code.sql_order_info.format(_num=_, date=order_date), read_conn,
    )
    order_info = pd.merge(order_info, first_order, on='user_id', how='left')
    order_info = pd.merge(order_info, _user_info, on='user_id', how='left')
    order_info = order_info.fillna(0)
    rd.insert_to_data(order_info, write_conn, write_order_db_name, write_order_tab_name)
    read_conn.close()
    write_conn.close()


# 同步维度表： 渠道，书，推广
def read_dict_table(read_conn_fig, write_conn_fig, date):
    print('======> is work to read -*- read_dict_table -*- ===> start:', dt.datetime.now())
    write_db_name = 'market_read'
    read_conn = rd.connect_database_host(read_conn_fig)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    # read_dict_all(read_conn, write_conn, sql_code.sql_dict_total_admin, write_db_name, 'admin_info')
    # read_dict_all(read_conn, write_conn, sql_code.sql_dict_total_book, write_db_name, 'book_info')
    # read_dict_all(read_conn, write_conn, sql_code.sql_book_channel_price, write_db_name, 'book_channel_price')
    # read_dict_update(read_conn, write_conn, sql_code.sql_dict_update_referral, write_db_name, 'referral_info', date)
    # read_custom_update(read_conn, write_conn, sql_code.sql_dict_update_custom, write_db_name, 'custom', date)
    # read_custom_url_collect(
    #     read_conn, write_conn, sql_code.sql_dict_update_custom_url_collect, write_db_name, 'custom_url_collect', date
    # )
    # read_custom_url_collect(
    #     read_conn, write_conn, sql_code.sql_dict_update_custom_url_activity, write_db_name, 'activity', date
    # )
    read_conn.close()
    write_conn.close()


def read_dict_all(read_conn, write_conn, read_sql, write_db_name, write_tab_name, fill_na=0):
    print('****** is time to write table: ', write_tab_name)
    data_info = pd.read_sql(
        read_sql, read_conn
    )
    data_info = data_info.fillna(fill_na)
    rd.insert_to_data(data_info, write_conn, write_db_name, write_tab_name)


def read_dict_update(read_conn, write_conn, read_sql, write_db_name, write_tab_name, date, fill_na=0):
    print('****** is time to write table: ', write_tab_name)
    if not date:
        date = rd.read_last_date(write_conn, write_db_name, write_tab_name, 'updatetime')
    data_info = pd.read_sql(
        read_sql.format(date=date), read_conn
    )
    data_info = data_info.fillna(fill_na)
    rd.insert_to_data(data_info, write_conn, write_db_name, write_tab_name)


def read_custom_update(read_conn, write_conn, read_sql, write_db_name, write_tab_name, date, fill_na=0):
    print('****** is time to write table: ', write_tab_name)
    if not date:
        date = rd.read_last_date(write_conn, write_db_name, write_tab_name, 'updatetime')
    custom_url = pd.read_sql(
        sql_code.sql_dict_update_custom_url.format(date=date), read_conn
    )
    custom = pd.read_sql(
        read_sql.format(date=date), read_conn
    )
    data_info = pd.merge(custom_url, custom, on='custom_id', how='left')
    data_info.fillna(fill_na, inplace=True)
    rd.subsection_insert_to_data(data_info, write_conn, write_db_name, write_tab_name)


def read_custom_url_collect(read_conn, write_conn, read_sql, write_db_name, write_tab_name, date, fill_na=0):
    print('****** is time to write table: ', write_tab_name)
    if not date:
        date = rd.read_last_date(write_conn, write_db_name, write_tab_name, 'updatetime')
    custom_url = pd.read_sql(
        read_sql.format(date=date), read_conn
    )
    custom_url.fillna(fill_na, inplace=True)
    rd.subsection_insert_to_data(custom_url, write_conn, write_db_name, write_tab_name)


def read_kd_log(write_conn_fig, write_db, write_tab, num, date=None, end_date=None):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=write_db, tab=write_tab, num=num), dt.datetime.now())
    read_conn_fig = rd.read_db_host(
        (os.path.split(os.path.realpath(__file__))[0] + '/config.yml')
    )
    read_host_conn_fig = cm.pick_conn_host_by_num(num, read_conn_fig['shart_host'])
    read_conn = rd.connect_database_direct(read_host_conn_fig)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    if not date:
        _before_yesterday = emdate.date_sub_days(2)
        date_name = emdate.block_date_list(_before_yesterday, end_date)[0]['date_name']
        _last_tab = write_tab + date_name + '_' + str(num)
        date = rd.read_last_date(write_conn, write_db, _last_tab, 'createtime')
    date_list = emdate.block_date_list(date, end_date, num=num)
    for _block in date_list:
        date_name, s_date, e_date = _block['date_name'], _block['s_date'], _block['e_date']
        block_data = _kd_log_read_data(read_conn, write_conn, s_date, e_date, num)
        write_tab_name = write_tab + date_name + '_' + str(num)
        rd.delete_last_date(write_conn, write_db, write_tab_name, 'createtime', s_date, e_date, date_type='stamp')
        rd.insert_to_data(block_data, write_conn, write_db, write_tab_name)
    read_conn.close()
    write_conn.close()


def _kd_log_read_data(read_conn, write_conn, s_date, e_date, num):
    user_info = pd.read_sql(
        sql_code.sql_user_info_kd_log.format(num=num), write_conn
    )
    order_log = pd.read_sql(
        sql_code.sql_order_log.format(s_date=s_date, e_date=e_date, num=num), read_conn
    )
    consume_log = pd.read_sql(
        sql_code.sql_consume_log.format(s_date=s_date, e_date=e_date, num=num), read_conn
    )
    sign_log = pd.read_sql(
        sql_code.sql_sign_log.format(s_date=s_date, e_date=e_date, num=num), read_conn
    )
    logon_log = pd.read_sql(
        sql_code.sql_logon_log.format(s_date=s_date, e_date=e_date, num=num), read_conn
    )
    _log = pd.concat([order_log, consume_log, sign_log, logon_log])
    _log['user_id'] = _log['user_id'].astype(int)
    user_info['user_id'] = user_info['user_id'].astype(int)
    _log_info = pd.merge(_log, user_info, on='user_id', how='left')
    _log_info.fillna(0, inplace=True)
    return _log_info


def read_user_recently_read(write_conn_fig, write_db, write_tab, num, date=None, end_date=None):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=write_db, tab=write_tab, num=num), dt.datetime.now())
    read_conn_fig = rd.read_db_host(
        (os.path.split(os.path.realpath(__file__))[0] + '/config.yml')
    )
    read_host_conn_fig = cm.pick_conn_host_by_num(num, read_conn_fig['shart_host'])
    read_conn = rd.connect_database_direct(read_host_conn_fig)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    if not date:
        date = read_lase_date_user_recently_read_data(write_conn, num)
    block_data = _user_recently_read_data(read_conn, write_conn, date, num)
    write_tab_name = write_tab + '_' + str(num)
    rd.delete_last_date(write_conn, write_db, write_tab_name, date_type_name='updatetime', date=date, date_type='stamp')
    rd.insert_to_data(block_data, write_conn, write_db, write_tab_name)


def _user_recently_read_data(read_conn, write_conn, s_date, num):
    user_info = pd.read_sql(
        sql_code.sql_user_info_kd_log.format(num=num), write_conn
    )
    book_channel_price = pd.read_sql(
        sql_code.sql_book_channel_price_local.format(num=num), write_conn
    )
    book_price = pd.read_sql(
        sql_code.sql_book_price_local, write_conn
    )
    recently_read_data = pd.read_sql(
        sql_code.sql_recently_read_data.format(s_date=s_date, num=num), read_conn
    )

    recently_read_data['book_id'] = recently_read_data['book_id'].astype(int)
    recently_read_data['user_id'] = recently_read_data['user_id'].astype(int)

    book_channel_price['book_id'] = book_channel_price['book_id'].astype(int)
    book_channel_price['channel_id'] = book_channel_price['channel_id'].astype(int)

    user_info['user_id'] = user_info['user_id'].astype(int)
    user_info['channel_id'] = user_info['channel_id'].astype(int)

    book_price['book_id'] = book_price['book_id'].astype(int)

    recently_read_data = pd.merge(recently_read_data, user_info, on='user_id', how='left')
    recently_read_data = pd.merge(recently_read_data, book_channel_price, on=['book_id', 'channel_id'], how='left')
    recently_read_data = pd.merge(recently_read_data, book_price, on='book_id', how='left')
    recently_read_data = recently_read_data.fillna(0)
    return recently_read_data


def read_lase_date_user_recently_read_data(write_conn, num):
    try:
        s_date = pd.read_sql(
            sql_code.sql_lase_date_user_recently_read_data.format(num=num), write_conn
        )['md'][0]
    except:
        s_date = '2019-01-01'
    return s_date


def syn_happy_seven_sound_shard(write_conn_fig, write_db, write_tab_list, num, date=None, end_date=None):
    sql_dict = {
        'user': sql_code.sql_user_info,
        'orders': sql_code.sql_order_info,
    }
    if not date:
        date = emdate.date_sub_days(sub_days=2)
    read_conn_fig = rd.read_db_host(
        (os.path.split(os.path.realpath(__file__))[0] + '/config.yml')
    )
    read_host_conn_fig = cm.pick_conn_host_by_num(num, read_conn_fig['shard_host_sound'])
    read_conn = rd.connect_database_direct(read_host_conn_fig)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    for tab in write_tab_list:
        print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
            db=write_db, tab=tab, num=num), dt.datetime.now())
        one_sql = sql_dict[tab]
        _one_happy_seven_sound_shard(
            read_conn=read_conn, write_conn=write_conn, read_sql=one_sql,
            write_db=write_db, write_tab=tab, num=num, s_date=date
        )
    read_conn.close()
    write_conn.close()


def _one_happy_seven_sound_shard(read_conn, write_conn, read_sql, write_db, write_tab, num, s_date):
    s_date = emdate.ensure_date_type_is_stamp(s_date)
    one_data = pd.read_sql(
        read_sql.format(date=s_date, _num=num), read_conn
    )
    one_data.fillna(0, inplace=True)
    rd.insert_to_data(one_data, write_conn, write_db, write_tab)


def syn_happy_seven_sound_cps(write_conn_fig, write_db):
    sql_dict = {
        'book': sql_code.sql_dict_total_book,
        'admin': sql_code.sql_dict_total_admin,
        'referral': sql_code.sql_dict_update_referral_sound,
        'channel_price': sql_code.sql_book_channel_price,
        'podcast_episodes': sql_code.sql_podcast_episodes,
        'podcasts': sql_code.sql_podcasts,
    }
    read_conn_fig = rd.read_db_host(
        (os.path.split(os.path.realpath(__file__))[0] + '/config.yml')
    )
    read_conn = rd.connect_database_direct(read_conn_fig['cps_host_sound'])
    write_conn = rd.connect_database_vpn(write_conn_fig)
    for tab in sql_dict.keys():
        print('======> is start to run {db}.{tab} - dict ===> start time:'.format(
            db=write_db, tab=tab), dt.datetime.now())
        one_data = pd.read_sql(
            sql_dict[tab], read_conn
        )
        one_data.fillna(0, inplace=True)
        rd.insert_to_data(one_data, write_conn, write_db, tab)
    read_conn.close()
    write_conn.close()


def syn_happy_seven_sound_shard_num(write_conn_fig, write_db, write_tab_list, num, date=None, end_date=None):
    if not end_date:
        end_date = emdate.datetime_format_code(dt.datetime.now())
    sql_dict = {
        'consume': sql_code.sql_consume_log,
        'sign': sql_code.sql_sign_log,
    }
    if not date:
        date = emdate.date_sub_days(sub_days=2)
    read_conn_fig = rd.read_db_host(
        (os.path.split(os.path.realpath(__file__))[0] + '/config.yml')
    )
    read_host_conn_fig = cm.pick_conn_host_by_num(num, read_conn_fig['shard_host_sound'])
    read_conn = rd.connect_database_direct(read_host_conn_fig)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    for tab in write_tab_list:
        print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
            db=write_db, tab=tab, num=num), dt.datetime.now())
        one_sql = sql_dict[tab]
        rd.delete_last_date_num(
            conn=write_conn, db_name=write_db, tab_name=tab, date_type_name='createtime',
            date=date, num_type_name='num', num=num, date_type='stamp'
        )
        _one_happy_seven_sound_shard_num(
            read_conn=read_conn, write_conn=write_conn, read_sql=one_sql,
            write_db=write_db, write_tab=tab, num=num, s_date=date, e_date=end_date
        )
    read_conn.close()
    write_conn.close()


def _one_happy_seven_sound_shard_num(read_conn, write_conn, read_sql, write_db, write_tab, num, s_date, e_date):
    s_date = emdate.ensure_date_type_is_stamp(s_date)
    one_data = pd.read_sql(
        read_sql.format(s_date=s_date, num=num, e_date=e_date), read_conn
    )
    # print(one_data.index.size)
    one_data.fillna(0, inplace=True)
    one_data['num'] = num
    rd.insert_to_data(one_data, write_conn, write_db, write_tab)


def read_sign_order_read(write_conn_fig, write_db, write_tab, num, date=None, end_date=None):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=write_db, tab=write_tab, num=num), dt.datetime.now())
    read_conn_fig = rd.read_db_host(
        (os.path.split(os.path.realpath(__file__))[0] + '/config.yml')
    )
    read_host_conn_fig = cm.pick_conn_host_by_num(num, read_conn_fig['shart_host'])
    read_conn = rd.connect_database_direct(read_host_conn_fig)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    if not date:
        date = '2021-03-18'
    if not end_date:
        end_date = emdate.datetime_format_code(dt.datetime.now())
    block_data = _read_one_sign_order_read(read_conn, s_date=date, num=num, e_date=end_date)
    # rd.delete_table_data(write_conn, write_db, write_tab)
    rd.insert_to_data(block_data, write_conn, write_db, write_tab)


def _read_one_sign_order_read(read_conn, s_date, num, e_date):
    sign_data = pd.read_sql(
        sql_code.sql_user_sign_count.format(s_date=s_date, num=num, e_date=e_date), read_conn
    )
    order_data = pd.read_sql(
        sql_code.sql_order_count.format(s_date=s_date, num=num, e_date=e_date), read_conn
    )
    user_info = pd.read_sql(
        sql_code.sql_user_id_channel.format(s_date=s_date, num=num, e_date=e_date), read_conn
    )
    sign_data['user_id'] = sign_data['user_id'].astype(int)
    order_data['user_id'] = order_data['user_id'].astype(int)
    user_info['user_id'] = user_info['user_id'].astype(int)

    re_date = pd.merge(sign_data, order_data, on='user_id', how='outer')
    re_date = pd.merge(re_date, user_info, on='user_id', how='left')
    re_date.fillna(0, inplace=True)
    return re_date


def delete_portrait_user_order(write_conn_fig, write_db, write_tab):
    write_conn = rd.connect_database_vpn(write_conn_fig)
    rd.delete_table_data(write_conn, write_db, write_tab)


def portrait_user_order_run(write_conn_fig, write_db, write_tab, num, date=None, end_date=None):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=write_db, tab=write_tab, num=num), dt.datetime.now())
    read_conn_fig = rd.read_db_host(
        (os.path.split(os.path.realpath(__file__))[0] + '/config.yml')
    )
    read_host_conn_fig = cm.pick_conn_host_by_num(num, read_conn_fig['shart_host'])
    read_conn = rd.connect_database_direct(read_host_conn_fig)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    if not date:
        date = '2019-01-01'
    if not end_date:
        end_date = emdate.datetime_format_code(dt.datetime.now())
    block_data = _read_one_portrait_user_order(read_conn, s_date=date, num=num, e_date=end_date)
    block_data['tab_num'] = num
    rd.insert_to_data(block_data, write_conn, write_db, write_tab)


def _read_one_portrait_user_order(read_conn, s_date, num, e_date):
    re_date = pd.read_sql(
        sql_code.sql_user_order_portrait.format(s_date=s_date, num=num, e_date=e_date), read_conn
    )
    re_date.fillna(0, inplace=True)
    return re_date


def portrait_user_order_run_admin_book(write_conn_fig, write_db, write_tab, num, date=None, end_date=None):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=write_db, tab=write_tab, num=num), dt.datetime.now())
    read_conn_fig = rd.read_db_host(
        (os.path.split(os.path.realpath(__file__))[0] + '/config.yml')
    )
    read_host_conn_fig = cm.pick_conn_host_by_num(num, read_conn_fig['shart_host'])
    read_conn = rd.connect_database_direct(read_host_conn_fig)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    if not date:
        date = '2019-01-01'
    if not end_date:
        end_date = emdate.datetime_format_code(dt.datetime.now())
    block_data = _read_one_portrait_user_order_admin_book(read_conn, s_date=date, num=num, e_date=end_date)
    block_data['tab_num'] = num
    rd.insert_to_data(block_data, write_conn, write_db, write_tab)
    write_conn.close()


def _read_one_portrait_user_order_admin_book(read_conn, s_date, num, e_date):
    re_date = pd.read_sql(
        sql_code.sql_user_order_portrait_admin_book.format(s_date=s_date, num=num, e_date=e_date), read_conn
    )
    re_date.fillna(0, inplace=True)
    return re_date


def portrait_user_order_run_admin_book_count(write_conn_fig, write_db, write_tab, date=None):
    print('======> is start to run {db}.{tab} - count ===> start time:'.format(
        db=write_db, tab=write_tab), dt.datetime.now())
    write_conn = rd.connect_database_vpn(write_conn_fig)

    write_tab = write_tab + '_count'
    rd.delete_table_data(write_conn, write_db, write_tab)

    _ladder_by_day_count(write_conn, write_db, write_tab)
    write_conn.close()


def _ladder_by_day_count(write_conn, write_db, write_tab, s_date=None, day_ladder=None):
    if not s_date:
        s_date = '2019-06-01'
    if not day_ladder:
        day_ladder = -15
    end_day = emdate.datetime_format_code(dt.datetime.now())
    while s_date <= end_day:
        _date = emdate.date_sub_days(day_ladder, s_date)
        print('======> is start to run {db}.{tab} - {_s} to {_e} ===> start time:'.format(
            db=write_db, tab=write_tab, _s=s_date, _e=_date), dt.datetime.now())
        count_data = pd.read_sql(
            sql_code.sql_user_order_portrait_admin_book_count.format(s_date=s_date, _date=_date), write_conn
        )
        if count_data.index.size > 0:
            count_data.fillna(0, inplace=True)
            rd.subsection_insert_to_data(count_data, write_conn, write_db, write_tab)
        s_date = emdate.date_sub_days(day_ladder, s_date)


def user_date_interval(read_config, db_name, tab_name, num, date_col, s_date=None, end_date=None):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=db_name, tab=tab_name, num=num), dt.datetime.now())
    write_conn = rd.connect_database_vpn('datamarket')
    if not s_date:
        s_date = '2019-01-01'
    if not end_date:
        end_date = emdate.datetime_format_code(dt.datetime.now())
    date_list = emdate.block_date_list(s_date, end_date, num=num)
    one_num_data = read_one_num_data(write_conn, date_list, num)
    write_tab = tab_name + '_' + str(num)
    rd.delete_last_date(write_conn, db_name, write_tab, date_col, s_date)
    rd.insert_to_data(one_num_data, write_conn, db_name, write_tab)


def read_one_num_data(write_conn, date_list, num):
    all_date_data = []
    for _block in date_list:
        date_name, s_date, e_date = _block['date_name'], _block['s_date'], _block['e_date']
        block_data = pd.read_sql(
            sql_code.sql_user_date_interval.format(_block=date_name, num=num), write_conn
        )
        all_date_data.append(block_data)
    all_date_df = pd.concat(all_date_data)
    all_date_df.sort_values(by=['user_id', 'date_day'], inplace=True)
    all_date_df.fillna(0, inplace=True)
    all_date_df['user_id'] = all_date_df['user_id'].astype(int)
    all_date_df['next_id'] = all_date_df['user_id'].shift(-1)
    all_date_df['next_date'] = all_date_df['date_day'].shift(-1)
    all_date_df['day_sub'] = all_date_df.apply(
        lambda x: _date_cat(x['user_id'], x['next_id'], x['next_date'], x['date_day']), axis=1
    )
    all_date_df.fillna(0, inplace=True)
    return all_date_df


def _date_cat(user_id, next_id, date_day, next_date):
    if user_id != next_id:
        next_date = emdate.datetime_format_code(dt.datetime.now())
    _sub = emdate.sub_date(next_date, date_day)
    return _sub

