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
        # self.host = {'host': '172.16.0.248', 'user': 'root', 'pw': 'Qiyue@123'}
        self.host = rd.connect_database_vpn('datamarket')
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

    def step_run_kwargs(self, func, process_num=16, run_num=512, interval=0.03, step=1, **kwargs):
        if isinstance(run_num, int):
            tars = [_ for _ in range(run_num)]
        else:
            tars = run_num
        tar_date_list = self.get_date()
        for _day in tar_date_list:
            print('****** Start to run: {d} - {tab} ******'.format(d=_day, tab=self.write_tab))
            cm.thread_work_kwargs(
                func=func, run_list=tars, read_config=self.host, db_name=self.write_db, tab_name=self.write_tab,
                s_date=tar_date_list, date_col=self.date_col, process_num=process_num, interval=interval, step=step,
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

    def get_date(self):
        tar_date_list = [0]
        if self.extend == 'list':
            tar_date_list = self.read_last_date()
        if self.extend == 'continue':
            tar_date_list = self.read_last_date(is_list=0)
        if self.extend == 'delete':
            tar_date_list = self.read_last_date(is_list=0)
            self.delete_last_date(tar_date_list)
        return tar_date_list

    def read_last_date(self, is_list=1, date_format='{Y}-{M}-{D}'):
        if self.s_date:
            _date = self.s_date
        else:
            conn = rd.connect_database_host(self.host['host'], self.host['user'], self.host['pw'])
            _date = rd.read_last_date(conn, self.write_db, self.write_tab, date_type_name=self.date_col)
            conn.close()
            self.s_date = _date
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


def count_order_test(num):
    sql = 'SELECT count(*) user_num FROM user_info.user_info_{num};'.format(num=num)


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
    s_date = emdate.date_sub_days(sub_days=31, _s_day=s_date)
    date_list = emdate.block_date_list(s_date)
    conn = rd.connect_database_vpn(read_config)
    keep_data = _one_retained_three_index_by_user_run(conn, date_list, s_date, num)
    rd.delete_last_date(conn, db_name, tab_name, date_col, s_date)
    rd.subsection_insert_to_data(keep_data, conn,db_name, tab_name)
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
    _logon_keep = pd.merge(logon, action, on=['user_id', 'book_id'], how='left')
    logon_keep = _logon_keep.groupby(by=['date_day', 'action_date', 'book_id', 'type']).count().reset_index()
    _order_keep = pd.merge(order, action, on=['user_id', 'book_id'], how='left')
    order_keep = _order_keep.groupby(by=['date_day', 'action_date', 'book_id', 'type']).count().reset_index()
    keep_data = pd.concat([logon_keep, order_keep])
    keep_data['tab_num'] = num
    keep_data['date_sub'] = keep_data.apply(lambda x: emdate.sub_date(x['date_day'], x['action_date']), axis=1)
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

