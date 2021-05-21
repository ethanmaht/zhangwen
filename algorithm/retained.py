# coding=utf8
import pandas as pd
from pandas import DataFrame as df
from emtools import sql_code
from emtools import currency_means as cm
from emtools import read_database as rd
from emtools import emdate
import datetime as dt
import random


def retain_date_day(conn, db_name, table, date):
    date_dict = emdate.date_num_dict(date, 30)
    first_day, last_day = list(date_dict.keys())[0], list(date_dict.keys())[-1]
    one_day = pd.read_sql(
        sql_code.sql_retain_date_day.format(
            db=db_name, tab=table, date=date,
        ),
        conn
    )
    day_30 = pd.read_sql(
        sql_code.sql_retain_date_day_30.format(
            db=db_name, tab=table, s_date=first_day, e_date=last_day,
        ),
        conn
    )
    day_30['user_id'].astype(int)
    day_30['_'] = 1
    day_30 = day_30.pivot_table(
        index='user_id', columns='date_day', values='_', fill_value=0
    ).reset_index()
    day_30['user_id'] = day_30['user_id'].astype(int)
    one_day['user_id'] = one_day['user_id'].astype(int)
    one_day = pd.merge(one_day, day_30, on='user_id', how='left')
    one_day.rename(columns=date_dict, inplace=True)
    one_day = cm.pad_col(one_day)
    one_day.fillna(0, inplace=True)
    return one_day


class KeepTableDay:
    def __init__(self, list_day):
        self.host = '172.16.0.248'
        self.table_ = 'user_day'
        self.s_date = None
        self.list_day = list_day
        self.write_db = 'market_read'
        self.write_tab = 'keep_table_day'

    def count_keep_table_day_run(self, process_num=16):
        if self.s_date:
            _date = self.s_date
        else:
            conn = rd.connect_database_host(self.host, 'root', 'Qiyue@123')
            _date = rd.read_last_date(conn, self.write_db, self.write_tab, date_type_name='date_day')
            conn.close()
        date_list = []
        tar_date_list = emdate.date_list(_date, e_date=dt.datetime.now(), format_code='{Y}-{M}-{D}')
        for _date in tar_date_list:
            date_list += emdate.date_list(_date, num=self.list_day, format_code='{Y}-{M}-{D}')
        date_list = list(set(date_list))
        date_list.sort()
        for _day in date_list:
            print('****** Start to run: {d} - count_keep_table_day ******'.format(d=_day))
            tars = [_ for _ in range(512)]
            cm.thread_work(self.one_day_run, _day, tars=tars, process_num=process_num, interval=0.03, step=1)

    def one_day_run(self, date, num):
        print('======> is run keep_table_day to date:', date, ' num:', num)
        conn = rd.connect_database_host(self.host, 'root', 'Qiyue@123')
        one_day_dict = {'date_day': date, 'tab_num': num}
        table_name = self.table_ + '_' + str(num)
        data_one = retain_date_day(conn, 'happy_seven', table_name, date)
        one_day_dict.update(count_keep_table_day_logon(data_one))
        one_day_dict.update(count_keep_table_day_active(data_one))
        one_day_dict.update(count_keep_table_day_order(data_one))
        one_day_dict = df(one_day_dict, index=[num])
        one_day_dict.fillna(0, inplace=True)
        one_day_dict['ud_id'] = one_day_dict.apply(lambda x: cm.user_date_id(x['date_day'], x['tab_num']), axis=1)
        one_day_dict['month_natural_week'] = one_day_dict['date_day'].apply(
            lambda x: emdate.datetime_format_code(x, code='{nmw}'))
        one_day_dict['year_month'] = one_day_dict['date_day'].apply(
            lambda x: emdate.datetime_format_code(x, code='{Y}-{M}'))
        rd.insert_to_data(one_day_dict, conn, self.write_db, self.write_tab)
        conn.close()


def count_keep_table_day_logon(_data):
    _data['logon'] = _data['logon'].astype(int)
    _logon = _data.loc[_data['logon'] > 0]
    all_logon = sum(_logon['logon'])
    logon_2 = sum(_logon['2'])
    logon_3 = sum(_logon['3'])
    logon_7 = sum(_logon['7'])
    logon_14 = sum(_logon['14'])
    logon_30 = sum(_logon['30'])
    return {
        'all_logon': all_logon,
        'logon_2': logon_2,
        'logon_3': logon_3,
        'logon_7': logon_7,
        'logon_14': logon_14,
        'logon_30': logon_30,
    }


def count_keep_table_day_active(_data):
    _active = _data
    all_active = _active.index.size
    active_2 = sum(_active['2'])
    active_3 = sum(_active['3'])
    active_7 = sum(_active['7'])
    active_14 = sum(_active['14'])
    active_30 = sum(_active['30'])
    return {
        'all_active': all_active,
        'active_2': active_2,
        'active_3': active_3,
        'active_7': active_7,
        'active_14': active_14,
        'active_30': active_30,
    }


def count_keep_table_day_order(_data):
    _data['order_success'] = _data['order_success'].astype(int)
    _order = _data.loc[_data['order_success'] > 0]
    all_order = _order.index.size
    order_2 = sum(_order['2'])
    order_3 = sum(_order['3'])
    order_7 = sum(_order['7'])
    order_14 = sum(_order['14'])
    order_30 = sum(_order['30'])
    return {
        'all_order': all_order,
        'order_2': order_2,
        'order_3': order_3,
        'order_7': order_7,
        'order_14': order_14,
        'order_30': order_30,
    }


class RunCount:
    def __init__(self, write_db, write_tab, date_col, extend='continue'):
        self.host = rd.read_db_config('datamarket')
        self.s_date = None
        self.write_db = write_db
        self.write_tab = write_tab
        self.date_col = date_col
        self.extend = extend
        self.refresh = None

    def step_run(self, func, process_num=16, run_num=512, interval=0.03, step=1, *args):
        if isinstance(run_num, int):
            tars = [_ for _ in range(run_num)]
        else:
            tars = run_num
        tar_date_list = self.get_date()
        for _day in tar_date_list:
            print('****** Start to run: {d} - {tab} ******'.format(d=_day, tab=self.write_tab))
            cm.thread_work(
                func, *args, self.host, self.write_db, self.write_tab, _day,
                tars=tars, process_num=process_num, interval=interval, step=step
            )

    def step_run_kwargs(
            self, func,
            date_sub=None, process_num=16, run_num=512, interval=0.03, step=1,
            front_func=None, follow_func=None, end_func=None,
            **kwargs
    ):
        tar_date_list = self.get_date(date_sub=date_sub)
        for _day in tar_date_list:
            print('****** Start to run: {d} - {tab} ******'.format(d=_day, tab=self.write_tab))
            if isinstance(run_num, int):
                tars = [_ for _ in range(run_num)]
            else:
                tars = run_num
            if front_func:
                front_func(
                    host=self.host, write_db=self.write_db, write_tab=self.write_tab,
                    date_type_name=self.date_col, date=_day,
                    **kwargs
                )
            cm.thread_work_kwargs(
                func=func, run_list=tars, read_config=self.host, db_name=self.write_db, tab_name=self.write_tab,
                s_date=_day, date_col=self.date_col, process_num=process_num, interval=interval, step=step,
                **kwargs
            )
            if follow_func:
                try:
                    follow_func(
                        host=self.host, write_db=self.write_db, write_tab=self.write_tab,
                        date_type_name=self.date_col, date=_day,
                        **kwargs
                    )
                except:
                    pass
        if end_func:
            end_func(
                host=self.host, write_db=self.write_db, write_tab=self.write_tab,
                date_type_name=self.date_col,
                **kwargs
            )

    def direct_run(self, func, *args):
        if self.date_col:
            tar_date_list = self.read_last_date(is_list=0)[0]
        else:
            tar_date_list = self.s_date
        if self.refresh:
            self.refresh_table()
        func(self.host, self.write_db, self.write_tab, self.date_col, tar_date_list, *args)

    def direct_run_kwargs(self, func, **kwargs):
        if self.date_col:
            tar_date_list = self.get_date()[0]
        else:
            tar_date_list = self.s_date
        if self.refresh:
            self.refresh_table()
        func(
            read_config=self.host, write_db=self.write_db, write_tab=self.write_tab,
            date_col=self.date_col, s_date=tar_date_list, **kwargs
        )

    def get_date(self, date_sub=None):
        tar_date_list = [0]
        if self.extend == 'list':
            tar_date_list = self.read_last_date(date_sub=date_sub)
            self.delete_last_date(tar_date_list[0])
        if self.extend == 'continue':
            tar_date_list = self.read_last_date(is_list=0, date_sub=date_sub)
        if self.extend == 'delete':
            tar_date_list = self.read_last_date(is_list=0, date_sub=date_sub)
            self.delete_last_date(tar_date_list)
        return tar_date_list

    def read_last_date(self, is_list=1, date_sub=None, date_format='{Y}-{M}-{D}'):
        if self.s_date:
            _date = self.s_date
        else:
            conn = rd.connect_database_host(self.host['host'], self.host['user'], self.host['pw'])
            _date = rd.read_last_date(conn, self.write_db, self.write_tab, date_type_name=self.date_col)
            conn.close()
            self.s_date = _date
        if date_sub:
            _date = emdate.date_sub_days(date_sub, _date)
        if is_list:
            _date = emdate.date_list(_date, e_date=dt.datetime.now(), format_code=date_format)
            _date.sort()
        else:
            _date = [_date]
        return _date

    def delete_last_date(self, del_date):
        del_db, del_tab, del_types = self.write_db, self.write_tab, self.date_col
        conn = rd.connect_database_host(self.host['host'], self.host['user'], self.host['pw'])
        del_date = del_date[0]
        rd.delete_last_date(conn, del_db, del_tab, del_types, del_date)
        conn.close()

    def refresh_table(self, del_db=None, del_tab=None):
        if not del_db:
            del_db = self.write_db
        if not del_tab:
            del_tab = self.write_tab
        conn = rd.connect_database_host(self.host['host'], self.host['user'], self.host['pw'])
        rd.delete_table_data(conn, del_db, del_tab)
        conn.close()


def ladder_by_day_count(func, write_conn, write_db, write_tab, s_date=None, day_ladder=None, **kwargs):
    if not s_date:
        s_date = '2019-07-01'
    if not day_ladder:
        day_ladder = -30
    end_day = emdate.datetime_format_code(dt.datetime.now())
    while s_date <= end_day:
        _date = emdate.date_sub_days(day_ladder, s_date)
        print('======> is start to run {db}.{tab} - {_s} to {_e} ===> start time:'.format(
            db=write_db, tab=write_tab, _s=s_date, _e=_date), dt.datetime.now())
        func(write_conn, write_db, write_tab, s_date, _date, **kwargs)
        s_date = _date


def count_order_logon_conversion(host, write_db, write_tab, date, num):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=write_db, tab=write_tab, num=num), dt.datetime.now())
    conn = rd.connect_database_host(host['host'], host['user'], host['pw'])
    first_order = pd.read_sql(sql_code.analysis_first_order.format(num=num, date=date), conn)
    repeat_order = pd.read_sql(sql_code.analysis_repeat_order.format(num=num, date=date), conn)
    all_order = pd.read_sql(sql_code.analysis_all_user_order.format(num=num, date=date), conn)
    vip_order = pd.read_sql(sql_code.analysis_vip_order.format(num=num, date=date), conn)
    first_repeat_order = pd.read_sql(sql_code.analysis_first_repeat_order.format(num=num, date=date), conn)
    logon_book_admin = pd.read_sql(sql_code.analysis_logon_book_admin.format(num=num, date=date), conn)
    one_num = pd.concat([logon_book_admin, all_order, first_order, repeat_order, first_repeat_order, vip_order])
    one_num = one_num.fillna(0)
    rd.insert_to_data(one_num, conn, write_db, write_tab)
    conn.close()


def compress_order_logon_conversion(host, write_db, write_tab, date_type_name, date):
    conn = rd.connect_database_host(host['host'], host['user'], host['pw'])
    compress_date = pd.read_sql(
        sql_code.analysis_compress_order_logon_conversion.format(
            db=write_db, tab=write_tab, date=date
        ),
        conn
    )
    compress_date['date_sub'] = compress_date.apply(lambda x: emdate.sub_date(x['logon_day'], x['order_day']), axis=1)
    compress_date = compress_date.fillna(0)
    rd.delete_last_date(conn, write_db, write_tab, date_type_name, date)
    rd.subsection_insert_to_data(compress_date, conn, write_db, write_tab)
    conn.close()


def make_sample_list(size, limit_max, limit_min=0):
    _limit = limit_max - limit_min + 1
    if size > _limit:
        size = _limit
    sample_list = []
    while len(sample_list) < size:
        sample_list.append(random.randint(limit_min, limit_max))
        sample_list = list(set(sample_list))
    sample_list.sort()
    return sample_list


def retained_three_index_by_user(read_config, db_name, tab_name, date_col, num, s_date=None):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=db_name, tab=tab_name, num=num), dt.datetime.now())
    if isinstance(s_date, list):
        s_date = s_date[0]
    date_list = emdate.block_date_list(s_date)
    conn = rd.connect_database_host(read_config['host'], read_config['user'], read_config['pw'])
    keep_data = _one_retained_three_index_by_user_run(conn=conn, date_list=date_list, s_date=s_date, num=num)
    keep_data = keep_data.fillna(0)
    rd.insert_to_data(keep_data, conn, db_name, tab_name)
    conn.close()


def _one_retained_three_index_by_user_run(conn, date_list, s_date, num):
    action = _read_one_num_data(
        sql_code.analysis_keep_action_by_date_block, conn=conn, date_list=date_list, s_date=s_date, num=num
    )
    logon = _read_one_num_data(
        sql_code.analysis_keep_logon_by_date_block, conn=conn, date_list=date_list, s_date=s_date, num=num
    )
    order = _read_one_num_data(
        sql_code.analysis_keep_order_by_date_block, conn=conn, date_list=date_list, s_date=s_date, num=num
    )
    logon_num = logon.groupby(by=['date_day', 'book_id', 'type']).count().reset_index()
    logon_num['date_sub'] = 0
    _logon_keep = pd.merge(logon, action, on=['user_id', 'book_id'], how='left')
    _logon_keep = _logon_keep.groupby(by=['date_day', 'action_date', 'book_id', 'type']).count().reset_index()

    order_num = order.groupby(by=['date_day', 'book_id', 'type']).count().reset_index()
    order_num['date_sub'] = 0
    _order_keep = pd.merge(order, action, on=['user_id', 'book_id'], how='left')
    _order_keep = _order_keep.groupby(by=['date_day', 'action_date', 'book_id', 'type']).count().reset_index()

    keep_data = pd.concat([_logon_keep, _order_keep])
    keep_data['action_date'] = keep_data.apply(lambda x: cm.fill_na_by_col(x['date_day'], x['action_date']), axis=1)
    keep_data['tab_num'] = num
    keep_data['date_sub'] = keep_data.apply(lambda x: emdate.sub_date(x['date_day'], x['action_date']), axis=1)
    keep_data = keep_data.loc[keep_data['date_sub'] > 0, :]
    keep_data = pd.concat([keep_data, order_num, logon_num])
    return keep_data


def _read_one_num_data(sql, conn, date_list, s_date, num):
    df_list = []
    for _date in date_list:
        _df = pd.read_sql(
            sql.format(date_code=_date['date_name'], num=num, s_date=s_date), conn
        )
        df_list.append(_df)
    complete_data = pd.concat(df_list)
    return complete_data


def count_keep_table_day_admin_run(read_config, db_name, tab_name, date_col, num, s_date):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=db_name, tab=tab_name, num=num), dt.datetime.now())
    conn = rd.connect_database_host(read_config['host'], read_config['user'], read_config['pw'])
    all_keep_one_day_run(conn, db_name, tab_name, date_col, s_date, num)


def all_keep_one_day_run(conn, db_name, tab_name, date_col, date, num):
    data_one = retain_date_day_admin(conn, date=date, num=num)
    order_keep = _keep_table_day_admin_typ(data_one, 'order_success', 'order_keep')
    order_keep.rename(
        columns={'2': 'order_2', '3': 'order_3', '7': 'order_7', '14': 'order_14', '30': 'order_30'}, inplace=True
    )
    logon_keep = _keep_table_day_admin_typ(data_one, 'logon', 'logon_keep')
    logon_keep.rename(
        columns={'2': 'logon_2', '3': 'logon_3', '7': 'logon_7', '14': 'logon_14', '30': 'logon_30'}, inplace=True
    )
    act_keep = _keep_table_day_admin_typ(data_one, 'all', 'all_keep')
    act_keep.rename(
        columns={'2': 'act_2', '3': 'act_3', '7': 'act_7', '14': 'act_14', '30': 'act_30'}, inplace=True
    )
    one_day = pd.merge(act_keep, order_keep, on=['date_day', 'admin_id'], how='left')
    one_day = pd.merge(one_day, logon_keep, on=['date_day', 'admin_id'], how='left')
    one_day.fillna(0, inplace=True)
    one_day['month_natural_week'] = one_day['date_day'].apply(
        lambda x: emdate.datetime_format_code(x, code='{nmw}'))
    one_day['year_month'] = one_day['date_day'].apply(
        lambda x: emdate.datetime_format_code(x, code='{Y}-{M}'))
    one_day['tab_num'] = num
    rd.insert_to_data(one_day, conn, db_name, tab_name)
    conn.close()


def retain_date_day_admin(conn, date, num):
    date_dict = emdate.date_num_dict(date, 30)
    first_day, last_day = list(date_dict.keys())[0], list(date_dict.keys())[-1]
    one_day = pd.read_sql(
        sql_code.sql_retain_date_day_num.format(num=num, date=date), conn
    )
    day_30 = pd.read_sql(
        sql_code.sql_retain_date_day_30_num.format(num=num, s_date=first_day, e_date=last_day), conn
    )
    user_info = pd.read_sql(
        sql_code.sql_keep_user_admin_id.format(num=num), conn
    )
    day_30['user_id'] = day_30['user_id'].astype(int)
    day_30.loc[:, '_'] = 1
    day_30 = day_30.pivot_table(
        index='user_id', columns='date_day', values='_', fill_value=0
    ).reset_index()
    day_30['user_id'] = day_30['user_id'].astype(int)
    one_day['user_id'] = one_day['user_id'].astype(int)
    user_info['user_id'] = user_info['user_id'].astype(int)
    user_info['admin_id'] = user_info['admin_id'].astype(int)
    one_day = pd.merge(one_day, day_30, on='user_id', how='left')
    one_day = pd.merge(one_day, user_info, on='user_id', how='left')
    one_day.rename(columns=date_dict, inplace=True)
    one_day = cm.pad_col(one_day)
    one_day.fillna(0, inplace=True)
    return one_day


def _keep_table_day_admin_typ(_data, col, typ_name):
    if col == 'all':
        _data.loc[:, typ_name] = 1
        _data_one = _data[['date_day', 'admin_id', typ_name, '2', '3', '7', '14', '30']]
        _data_one = _data_one.groupby(by=['date_day', 'admin_id']).sum().reset_index()
    else:
        _data[col] = _data[col].astype(int)
        _data_one = _data.loc[_data[col] > 0, :]
        _data_one.loc[:, typ_name] = 1
        _data_one = _data_one[['date_day', 'admin_id', typ_name, '2', '3', '7', '14', '30']]
        _data_one = _data_one.groupby(by=['date_day', 'admin_id']).sum().reset_index()
    return _data_one


def keep_day_admin_count(host, write_db, write_tab, date_type_name, date):
    conn = rd.connect_database_host(host['host'], host['user'], host['pw'])
    compress_date = pd.read_sql(
        sql_code.sql_keep_day_admin_count.format(db=write_db, tab=write_tab, date=date), conn
    )
    admin_info = pd.read_sql(
        sql_code.sql_keep_admin_id_name, conn
    )
    compress_date['admin_id'] = compress_date['admin_id'].astype(int)
    admin_info['admin_id'] = admin_info['admin_id'].astype(int)
    compress_date = pd.merge(compress_date, admin_info, on='admin_id', how='left')
    compress_date = compress_date.fillna(0)
    rd.delete_last_date(conn, write_db, write_tab, date_type_name, date)
    rd.subsection_insert_to_data(compress_date, conn, write_db, write_tab)
    conn.close()


def user_keep_admin_run(read_config, db_name, tab_name, date_col, num, s_date):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=db_name, tab=tab_name, num=num), dt.datetime.now())
    conn = rd.connect_database_host(read_config['host'], read_config['user'], read_config['pw'])
    user_keep_admin(conn=conn, db_name=db_name, tab_name=tab_name, date_col=date_col, date=s_date, num=num)
    conn.close()


def user_keep_admin(conn, db_name, tab_name, date_col, date, num, keep_pool=None):
    date_list = emdate.block_date_list(date)
    sql = sql_code.sql_keep_book_admin_date_num
    one_num_data = rd.read_date_block(conn, sql, date_list, num)
    one_num_data = merge_keep_day_data(one_num_data, keep_pool)
    one_num_data_count = one_num_data.groupby(
        by=['date_day', 'channel_id', 'book_id', 'type']
    ).sum().reset_index()
    one_num_data_count['tab_num'] = num
    rd.insert_to_data(one_num_data_count, conn, db_name, tab_name)


def merge_keep_day_data(df_data, keep_pool=None):
    if not keep_pool:
        keep_pool = [2, 3, 7, 14, 30]
    df_data['date_day'] = df_data['date_day'].apply(lambda x: emdate.datetime_format_code(x))
    df_data['user_id'].astype(int)
    for _day_sub in keep_pool:
        mid_keep = df_data[['date_day', 'user_id', 'channel_id', 'book_id']]
        mid_keep = mid_keep.drop_duplicates()
        mid_keep = mid_keep.reset_index().drop(columns='index')
        mid_keep.loc[:, 'keep'] = 1
        _sub = _day_sub - 1
        mid_keep['date_day'] = mid_keep.apply(lambda x: emdate.date_sub_days(_sub, x['date_day']), axis=1)
        df_data = pd.merge(df_data, mid_keep, on=['date_day', 'user_id', 'channel_id', 'book_id'], how='left')
        df_data.rename(columns={'keep': 'keep_{_day}'.format(_day=_day_sub)}, inplace=True)
    return df_data


def user_keep_admin_count(host, write_db, write_tab, date_type_name, date):
    conn = rd.connect_database_host(host['host'], host['user'], host['pw'])
    count_date = pd.read_sql(
        sql_code.sql_keep_book_admin_count.format(db=write_db, tab=write_tab, date=date), conn
    )
    admin_info = pd.read_sql(
        sql_code.sql_keep_admin_id_name, conn
    )
    book_info = pd.read_sql(
        sql_code.sql_keep_book_id_name, conn
    )
    count_date['channel_id'] = count_date['channel_id'].astype(int)
    admin_info['admin_id'] = admin_info['admin_id'].astype(int)

    count_date['book_id'] = count_date['book_id'].astype(int)
    book_info['book_id'] = book_info['book_id'].astype(int)

    all_date = pd.merge(count_date, admin_info, left_on='channel_id', right_on='admin_id', how='left')
    all_date = pd.merge(all_date, book_info, on='book_id', how='left')

    all_date.fillna(0, inplace=True)

    rd.delete_last_date(conn, write_db, write_tab, date_type_name, date)
    rd.subsection_insert_to_data(all_date, conn, write_db, write_tab)

    conn.close()


def retained_three_index_by_user_count(host, write_db, write_tab, date_type_name, date):
    conn = rd.connect_database_host(host['host'], host['user'], host['pw'])
    compress_date = pd.read_sql(
        sql_code.sql_retained_three_index_by_user_count.format(db=write_db, tab=write_tab, date=date), conn
    )
    book_info = pd.read_sql(
        sql_code.sql_retained_three_index_by_user_count_book_info, conn
    )
    compress_date['book_id'] = compress_date['book_id'].astype(int)
    book_info['book_id'] = book_info['book_id'].astype(int)
    compress_date = pd.merge(compress_date, book_info, on='book_id', how='left')
    compress_date = compress_date.fillna(0)
    rd.delete_last_date(conn, write_db, write_tab, date_type_name, date)
    rd.subsection_insert_to_data(compress_date, conn, write_db, write_tab)
    conn.close()


def retained_logon_compress_thirty_day(read_config, db_name, tab_name, date_col, num, s_date=None):
    print('======> is start to run {db}.{tab} - {num} ===> start time:'.format(
        db=db_name, tab=tab_name, num=num), dt.datetime.now())
    if isinstance(s_date, list):
        s_date = s_date[0]
    date_list = emdate.block_date_list(s_date)
    conn = rd.connect_database_host(read_config['host'], read_config['user'], read_config['pw'])
    keep_data = _one_retained_logon_compress_thirty_day(conn=conn, date_list=date_list, s_date=s_date, num=num)
    keep_data = keep_data.fillna(0)
    rd.insert_to_data(keep_data, conn, db_name, tab_name)
    conn.close()


def _one_retained_logon_compress_thirty_day(conn, date_list, s_date, num):
    action = _read_one_num_data(
        sql_code.analysis_retained_logon_compress_thirty_day, conn=conn, date_list=date_list, s_date=s_date, num=num
    )
    action['tab_num'] = num
    action['date_sub'] = action.apply(lambda x: emdate.sub_date(x['logon_date'], x['date_day']), axis=1)
    return action


def retained_logon_compress_thirty_day_count(host, write_db, write_tab, date_type_name, date):
    conn = rd.connect_database_host(host['host'], host['user'], host['pw'])
    compress_date = pd.read_sql(
        sql_code.sql_retained_logon_compress_thirty_day_count.format(db=write_db, tab=write_tab, date=date), conn
    )
    book_info = pd.read_sql(
        sql_code.sql_retained_three_index_by_user_count_book_info, conn
    )
    admin_info = pd.read_sql(
        sql_code.sql_retained_admin_info, conn
    )
    compress_date['book_id'] = compress_date['book_id'].astype(int)
    book_info['book_id'] = book_info['book_id'].astype(int)
    compress_date['channel_id'] = compress_date['channel_id'].astype(int)
    admin_info['channel_id'] = admin_info['channel_id'].astype(int)
    compress_date = pd.merge(compress_date, book_info, on='book_id', how='left')
    compress_date = pd.merge(compress_date, admin_info, on='channel_id', how='left')
    compress_date = compress_date.fillna(0)
    rd.delete_last_date(conn, write_db, write_tab, date_type_name, date)
    rd.subsection_insert_to_data(compress_date, conn, write_db, write_tab)
    conn.close()


def chart_book_admin_read(read_config, db_name, tab_name, date_col, num, s_date=None):
    if isinstance(s_date, list):
        s_date = s_date[0]
    print('======> is start to run {db}.{tab} - {num} - {date} ===> start time:'.format(
        db=db_name, tab=tab_name, date=s_date, num=num), dt.datetime.now())
    conn = rd.connect_database_host(read_config['host'], read_config['user'], read_config['pw'])
    read_data = pd.read_sql(
        sql_code.sql_book_admin_read.format(date=s_date, num=num), conn
    )
    read_data['tab_num'] = num
    read_data = read_data.fillna(0)
    rd.insert_to_data(read_data, conn, db_name, tab_name)
    conn.close()


def chart_book_admin_read_count(host, write_db, write_tab, date_type_name, date):
    print('======> is start to run {db}.{tab} - count - {date} ===> start time:'.format(
        db=write_db, tab=write_tab, date=date), dt.datetime.now())
    conn = rd.connect_database_host(host['host'], host['user'], host['pw'])
    read_date = pd.read_sql(
        sql_code.sql_book_admin_read_count.format(db=write_db, tab=write_tab, date=date), conn
    )
    book_info = pd.read_sql(
        sql_code.sql_retained_three_index_by_user_count_book_info, conn
    )
    admin_info = pd.read_sql(
        sql_code.sql_retained_admin_info, conn
    )
    read_date['book_id'] = read_date['book_id'].astype(int)
    book_info['book_id'] = book_info['book_id'].astype(int)
    read_date['channel_id'] = read_date['channel_id'].astype(int)
    admin_info['channel_id'] = admin_info['channel_id'].astype(int)
    read_date = pd.merge(read_date, book_info, on='book_id', how='left')
    read_date = pd.merge(read_date, admin_info, on='channel_id', how='left')
    read_date = read_date.fillna(0)
    rd.delete_last_date(conn, write_db, write_tab, date_type_name, date)
    rd.subsection_insert_to_data(read_date, conn, write_db, write_tab)
    conn.close()


def chart_book_admin_read_30(read_config, db_name, tab_name, date_col, num, s_date=None):
    if isinstance(s_date, list):
        s_date = s_date[0]
    print('======> is start to run {db}.{tab} - {num} - {date} ===> start time:'.format(
        db=db_name, tab=tab_name, date=s_date, num=num), dt.datetime.now())
    conn = rd.connect_database_host(read_config['host'], read_config['user'], read_config['pw'])
    read_data = pd.read_sql(
        sql_code.sql_book_admin_read_step_30.format(date=s_date, num=num), conn
    )
    read_data['tab_num'] = num
    read_data = read_data.fillna(0)
    rd.insert_to_data(read_data, conn, db_name, tab_name)
    conn.close()


def chart_book_admin_read_count_30(host, write_db, write_tab, date_type_name, date):
    print('======> is start to run {db}.{tab} - count - {date} ===> start time:'.format(
        db=write_db, tab=write_tab, date=date), dt.datetime.now())
    conn = rd.connect_database_host(host['host'], host['user'], host['pw'])
    ladder_by_day_count(
        chart_book_admin_read_count_30_day_ladder, conn, write_db, write_tab, date_type_name=date_type_name
    )
    conn.close()
    # read_date = pd.read_sql(
    #     sql_code.sql_book_admin_read_count_30.format(db=write_db, tab=write_tab, date=date), conn
    # )
    # book_info = pd.read_sql(
    #     sql_code.sql_retained_three_index_by_user_count_book_info, conn
    # )
    # admin_info = pd.read_sql(
    #     sql_code.sql_retained_admin_info, conn
    # )
    # read_date['book_id'] = read_date['book_id'].astype(int)
    # book_info['book_id'] = book_info['book_id'].astype(int)
    # read_date['channel_id'] = read_date['channel_id'].astype(int)
    # admin_info['channel_id'] = admin_info['channel_id'].astype(int)
    # read_date = pd.merge(read_date, book_info, on='book_id', how='left')
    # read_date = pd.merge(read_date, admin_info, on='channel_id', how='left')
    # read_date = read_date.fillna(0)
    # rd.delete_last_date(conn, write_db, write_tab, date_type_name, date)
    # rd.subsection_insert_to_data(read_date, conn, write_db, write_tab)
    # conn.close()


def chart_book_admin_read_count_30_day_ladder(conn, write_db, write_tab, s_date, _date, date_type_name):
    print('======> is start to run {db}.{tab} - count - {s_date} to {_date} ===> start time:'.format(
        db=write_db, tab=write_tab, s_date=s_date, _date=_date), dt.datetime.now())
    read_date = pd.read_sql(
        sql_code.sql_book_admin_read_count_30_day_ladder.format(
            db=write_db, tab=write_tab, s_date=s_date, e_date=_date
        ), conn
    )
    book_info = pd.read_sql(
        sql_code.sql_retained_three_index_by_user_count_book_info, conn
    )
    admin_info = pd.read_sql(
        sql_code.sql_retained_admin_info, conn
    )
    read_date['book_id'] = read_date['book_id'].astype(int)
    book_info['book_id'] = book_info['book_id'].astype(int)
    read_date['channel_id'] = read_date['channel_id'].astype(int)
    admin_info['channel_id'] = admin_info['channel_id'].astype(int)
    read_date = pd.merge(read_date, book_info, on='book_id', how='left')
    read_date = pd.merge(read_date, admin_info, on='channel_id', how='left')
    read_date = read_date.fillna(0)
    write_tab = write_tab + '_count'
    rd.delete_last_date(conn, write_db, write_tab, date_type_name, s_date)
    rd.subsection_insert_to_data(read_date, conn, write_db, write_tab)


def chart_book_admin_read_new_user_30(read_config, db_name, tab_name, date_col, num, s_date=None):
    if isinstance(s_date, list):
        s_date = s_date[0]
    print('======> is start to run {db}.{tab} - {num} - {date} ===> start time:'.format(
        db=db_name, tab=tab_name, date=s_date, num=num), dt.datetime.now())
    conn = rd.connect_database_host(read_config['host'], read_config['user'], read_config['pw'])
    read_data = pd.read_sql(
        sql_code.sql_book_admin_read_step_new_user_30.format(date=s_date, num=num), conn
    )
    read_data['tab_num'] = num
    read_data = read_data.fillna(0)
    rd.insert_to_data(read_data, conn, db_name, tab_name)
    conn.close()


def chart_book_admin_conversion_funnel(read_config, db_name, tab_name, num, date_col, s_date=None):
    if isinstance(s_date, list):
        s_date = s_date[0]
    print('======> is start to run {db}.{tab} - {num} - {date} ===> start time:'.format(
        db=db_name, tab=tab_name, date=s_date, num=num), dt.datetime.now())
    conn = rd.connect_database_host(read_config['host'], read_config['user'], read_config['pw'])

    _consume = _read_conversion_funnel_by_block(conn=conn, date=s_date, num=num)
    logon_user = pd.read_sql(
        sql_code.sql_logon_user.format(s_date=s_date, num=num), conn
    )
    user_read = pd.read_sql(
        sql_code.sql_user_read.format(s_date=s_date, num=num), conn
    )
    first_order = pd.read_sql(
        sql_code.sql_first_order.format(s_date=s_date, num=num), conn
    )
    recharge_order = pd.read_sql(
        sql_code.sql_first_recharge_order.format(s_date=s_date, num=num), conn
    )

    logon_user[['user_id', 'book_id', 'is_subscribe']] = logon_user[['user_id', 'book_id', 'is_subscribe']].astype(int)
    user_read[['user_id', 'book_id']] = user_read[['user_id', 'book_id']].astype(int)
    first_order['user_id'] = first_order['user_id'].astype(int)
    recharge_order['user_id'] = recharge_order['user_id'].astype(int)
    _consume['user_id'] = _consume['user_id'].astype(int)

    read_data = pd.merge(logon_user, user_read, on=['user_id', 'book_id'], how='left')
    read_data = pd.merge(read_data, first_order, on=['user_id'], how='left')
    read_data = pd.merge(read_data, recharge_order, on=['user_id'], how='left')
    read_data = pd.merge(read_data, _consume, on=['user_id'], how='left')

    read_data['first_sub'] = read_data.apply(lambda x: emdate.sub_date(x['logon_date'], x['first_time']), axis=1)
    read_data['recharge_sub'] = read_data.apply(lambda x: emdate.sub_date(x['logon_date'], x['recharge_time']), axis=1)

    group_data = read_data.groupby(
        by=['logon_date', 'book_id', 'admin_id', 'first_sub', 'recharge_sub'],
    )[['is_subscribe', 'logon_user', 'pass_free', 'first_order', 'recharge_order']].sum()

    group_data = group_data.reset_index()
    group_data['tab_num'] = num
    group_data = group_data.fillna(0)
    rd.insert_to_data(group_data, conn, db_name, tab_name)
    conn.close()


def _read_conversion_funnel_by_block(conn, date, num):
    date_list = emdate.block_date_list(date)
    sql = sql_code.sql_consume
    one_num_data = rd.read_date_block(conn, sql, date_list, num)
    one_num_data = one_num_data.drop_duplicates(['user_id'])
    one_num_data['consume'] = 1
    return one_num_data


def conversion_funnel_count(host, write_db, write_tab, date_type_name, date):
    print('======> is start to run {db}.{tab} - count - {date} ===> start time:'.format(
        db=write_db, tab=write_tab, date=date), dt.datetime.now())
    conn = rd.connect_database_host(host['host'], host['user'], host['pw'])
    read_date = pd.read_sql(
        sql_code.sql_conversion_funnel_count.format(db=write_db, tab=write_tab, date=date), conn
    )
    book_info = pd.read_sql(
        sql_code.sql_retained_three_index_by_user_count_book_info, conn
    )
    admin_info = pd.read_sql(
        sql_code.sql_retained_admin_info, conn
    )
    read_date['book_id'] = read_date['book_id'].astype(int)
    book_info['book_id'] = book_info['book_id'].astype(int)
    read_date['channel_id'] = read_date['channel_id'].astype(int)
    admin_info['channel_id'] = admin_info['channel_id'].astype(int)
    read_date = pd.merge(read_date, book_info, on='book_id', how='left')
    read_date = pd.merge(read_date, admin_info, on='channel_id', how='left')
    read_date = read_date.fillna(0)

    write_tab = write_tab + '_count'
    rd.delete_last_date(conn, write_db, write_tab, date_type_name, date)
    rd.subsection_insert_to_data(read_date, conn, write_db, write_tab)
    conn.close()


def chart_book_admin_conversion_funnel_all_book(read_config, db_name, tab_name, num, date_col, s_date=None):
    if isinstance(s_date, list):
        s_date = s_date[0]
    print('======> is start to run {db}.{tab} - {num} - {date} ===> start time:'.format(
        db=db_name, tab=tab_name, date=s_date, num=num), dt.datetime.now())
    conn = rd.connect_database_host(read_config['host'], read_config['user'], read_config['pw'])

    _consume = _read_conversion_funnel_by_block_not_same_book(conn=conn, date=s_date, num=num)
    logon_user = pd.read_sql(
        sql_code.sql_logon_user.format(s_date=s_date, num=num), conn
    )
    user_read = pd.read_sql(
        sql_code.sql_user_read_not_same_book.format(s_date=s_date, num=num), conn
    )
    first_order = pd.read_sql(
        sql_code.sql_first_order_not_same_book.format(s_date=s_date, num=num), conn
    )
    recharge_order = pd.read_sql(
        sql_code.sql_first_recharge_order_not_same_book.format(s_date=s_date, num=num), conn
    )

    logon_user[['user_id', 'book_id', 'is_subscribe']] = logon_user[['user_id', 'book_id', 'is_subscribe']].astype(int)
    user_read[['user_id', 'book_id']] = user_read[['user_id', 'book_id']].astype(int)
    first_order['user_id'] = first_order['user_id'].astype(int)
    recharge_order['user_id'] = recharge_order['user_id'].astype(int)
    _consume['user_id'] = _consume['user_id'].astype(int)

    read_data = pd.merge(logon_user, user_read, on=['user_id', 'book_id'], how='left')
    read_data = pd.merge(read_data, first_order, on=['user_id'], how='left')
    read_data = pd.merge(read_data, recharge_order, on=['user_id'], how='left')
    read_data = pd.merge(read_data, _consume, on=['user_id'], how='left')

    read_data['first_sub'] = read_data.apply(lambda x: emdate.sub_date(x['logon_date'], x['first_time']), axis=1)
    read_data['recharge_sub'] = read_data.apply(lambda x: emdate.sub_date(x['logon_date'], x['recharge_time']), axis=1)

    group_data = read_data.groupby(
        by=['logon_date', 'book_id', 'admin_id', 'first_sub', 'recharge_sub'],
    )[['is_subscribe', 'logon_user', 'pass_free', 'first_order', 'recharge_order']].sum()

    group_data = group_data.reset_index()
    group_data['tab_num'] = num
    group_data = group_data.fillna(0)
    rd.insert_to_data(group_data, conn, db_name, tab_name)
    conn.close()


def _read_conversion_funnel_by_block_not_same_book(conn, date, num):
    date_list = emdate.block_date_list(date)
    sql = sql_code.sql_consume_not_same_book
    one_num_data = rd.read_date_block(conn, sql, date_list, num)
    one_num_data = one_num_data.drop_duplicates(['user_id'])
    one_num_data['consume'] = 1
    return one_num_data


def index_data(read_config, db_name, tab_name, num, date_col, s_date=None, end_date=None):
    print('======> is start to run {db}.{tab} - {num} - {date} ===> start time:'.format(
        db=db_name, tab=tab_name, date=s_date, num=num), dt.datetime.now())
    write_conn = rd.connect_database_vpn('datamarket')
    if not s_date:
        s_date = '2019-01-01'
    if not end_date:
        end_date = emdate.datetime_format_code(dt.datetime.now())
    logon_user = pd.read_sql(
        sql_code.sql_logon_users.format(s_date=s_date, num=num), write_conn
    )
    order_user = pd.read_sql(
        sql_code.sql_order_users_money.format(s_date=s_date, num=num), write_conn
    )
    active_user = pd.read_sql(
        sql_code.sql_active.format(s_date=s_date, num=num), write_conn
    )
    back_user = pd.read_sql(
        sql_code.sql_back.format(s_date=s_date, num=num), write_conn
    )
    date_list = emdate.block_date_list(s_date, end_date, num=num)
    consume = read_one_num_data(write_conn, date_list, s_date, num)
    _index_data = pd.merge(logon_user, order_user, on='date_day', how='outer')
    _index_data = pd.merge(_index_data, active_user, on='date_day', how='outer')
    _index_data = pd.merge(_index_data, back_user, on='date_day', how='outer')
    _index_data = pd.merge(_index_data, consume, on='date_day', how='outer')
    _index_data.fillna(0, inplace=True)
    _index_data['tab_num'] = num
    _index_data['id'] = _index_data.apply(lambda x: cm.user_date_id(x['date_day'], x['tab_num']), axis=1)
    rd.insert_to_data(_index_data, write_conn, db_name, tab_name)
    write_conn.close()


def read_one_num_data(write_conn, date_list, s_date, num):
    all_date_data = []
    for _block in date_list:
        date_name, _, e_date = _block['date_name'], _block['s_date'], _block['e_date']
        block_data = pd.read_sql(
            sql_code.sql_user_date_consume.format(_block=date_name, s_date=s_date, num=num), write_conn
        )
        all_date_data.append(block_data)
    all_date_df = pd.concat(all_date_data)
    all_date_df.fillna(0, inplace=True)
    return all_date_df


def index_data_month(read_config, db_name, tab_name, num, date_col, s_date=None, end_date=None):
    print('======> is start to run {db}.{tab} - {num} - {date} ===> start time:'.format(
        db=db_name, tab=tab_name, date=s_date, num=num), dt.datetime.now())
    write_conn = rd.connect_database_vpn('datamarket')
    if not s_date:
        s_date = '2019-01-01'
    if not end_date:
        end_date = emdate.datetime_format_code(dt.datetime.now())
    logon_user = pd.read_sql(
        sql_code.sql_logon_users_ym.format(s_date=s_date, num=num), write_conn
    )
    order_user = pd.read_sql(
        sql_code.sql_order_users_money_ym.format(s_date=s_date, num=num), write_conn
    )
    active_user = pd.read_sql(
        sql_code.sql_active_ym.format(s_date=s_date, num=num), write_conn
    )
    back_user = pd.read_sql(
        sql_code.sql_back_ym.format(s_date=s_date, num=num), write_conn
    )
    date_list = emdate.block_date_list(s_date, end_date, num=num)
    consume = read_one_num_data_month(write_conn, date_list, s_date, num)
    _index_data = pd.merge(logon_user, order_user, on='date_month', how='outer')
    _index_data = pd.merge(_index_data, active_user, on='date_month', how='outer')
    _index_data = pd.merge(_index_data, back_user, on='date_month', how='outer')
    _index_data = pd.merge(_index_data, consume, on='date_month', how='outer')
    _index_data.fillna(0, inplace=True)
    _index_data['tab_num'] = num
    _index_data['id'] = _index_data.apply(lambda x: cm.user_date_id(x['date_month'], x['tab_num']), axis=1)
    rd.insert_to_data(_index_data, write_conn, db_name, tab_name)
    write_conn.close()


def read_one_num_data_month(write_conn, date_list, s_date, num):
    all_date_data = []
    for _block in date_list:
        date_name, _, e_date = _block['date_name'], _block['s_date'], _block['e_date']
        try:
            block_data = pd.read_sql(
                sql_code.sql_user_date_consume_ym.format(_block=date_name, s_date=s_date, num=num), write_conn
            )
            all_date_data.append(block_data)
        except:
            pass
    all_date_df = pd.concat(all_date_data)
    all_date_df.fillna(0, inplace=True)
    return all_date_df


def index_data_book_consume(read_config, db_name, tab_name, num, date_col, s_date=None, end_date=None):
    print('======> is start to run {db}.{tab} - {num} - {date} ===> start time:'.format(
        db=db_name, tab=tab_name, date=s_date, num=num), dt.datetime.now())
    write_conn = rd.connect_database_vpn('datamarket')
    if not s_date:
        s_date = '2019-01-01'
    if not end_date:
        end_date = emdate.datetime_format_code(dt.datetime.now())
    date_list = emdate.block_date_list(s_date, end_date, num=num)
    consume = read_one_num_data_book_consume(write_conn, date_list, s_date, num)
    consume.fillna(0, inplace=True)
    consume['tab_num'] = num
    rd.insert_to_data(consume, write_conn, db_name, tab_name)
    write_conn.close()


def read_one_num_data_book_consume(write_conn, date_list, s_date, num):
    all_date_data = []
    for _block in date_list:
        date_name, _, e_date = _block['date_name'], _block['s_date'], _block['e_date']
        block_data = pd.read_sql(
            sql_code.sql_book_date_consume.format(_block=date_name, s_date=s_date, num=num), write_conn
        )
        all_date_data.append(block_data)
    all_date_df = pd.concat(all_date_data)
    all_date_df.fillna(0, inplace=True)
    return all_date_df


def order_book_date_sub(read_config, db_name, tab_name, date_col, num, s_date=None):
    if isinstance(s_date, list):
        s_date = s_date[0]
    print('======> is start to run {db}.{tab} - {num} - {date} ===> start time:'.format(
        db=db_name, tab=tab_name, date=s_date, num=num), dt.datetime.now())
    conn = rd.connect_database_host(read_config['host'], read_config['user'], read_config['pw'])
    order_data = pd.read_sql(
        sql_code.sql_order_save.format(num=num, s_date=s_date), conn
    )
    book_info = pd.read_sql(
        sql_code.sql_retained_three_index_by_user_count_book_info, conn
    )
    admin_info = pd.read_sql(
        sql_code.sql_retained_admin_info, conn
    )
    order_data['book_id'] = order_data['book_id'].astype(int)
    book_info['book_id'] = book_info['book_id'].astype(int)
    order_data['channel_id'] = order_data['channel_id'].astype(int)
    admin_info['channel_id'] = admin_info['channel_id'].astype(int)
    read_date = pd.merge(order_data, book_info, on='book_id', how='left')
    read_date = pd.merge(read_date, admin_info, on='channel_id', how='left')
    read_date = read_date.fillna(0)

    rd.insert_to_data(read_date, conn, db_name, tab_name)
    conn.close()


def conversion_message_push(read_config, db_name, tab_name):
    conn = rd.connect_database_vpn(read_config)
    custom = pd.read_sql(sql_code.sql_custom, conn)
    url_collect = pd.read_sql(sql_code.sql_custom_url, conn)
    custom = pd.merge(custom, url_collect, on='custom_id', how='left')
    mp_send = pd.read_sql(sql_code.sql_mp_send, conn)
    template = pd.read_sql(sql_code.sql_templatemessage, conn)
    all_message = pd.concat([custom, mp_send, template])
    book = pd.read_sql(sql_code.sql_retained_three_index_by_user_count_book_info, conn)
    admin = pd.read_sql(sql_code.sql_retained_admin_info, conn)

    all_message['book_id'] = all_message['book_id'].astype(str)
    book['book_id'] = book['book_id'].astype(str)

    all_message['channel_id'] = all_message['channel_id'].astype(str)
    admin['channel_id'] = admin['channel_id'].astype(str)

    all_message = pd.merge(all_message, book, on='book_id', how='left')
    all_message = pd.merge(all_message, admin, on='channel_id', how='left')
    all_message.fillna(0, inplace=True)
    rd.insert_to_data(all_message, conn, db_name, tab_name)
