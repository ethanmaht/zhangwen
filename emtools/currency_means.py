import pandas as pd
import datetime as dt
from emtools import emdate
import threading
import time
from multiprocessing import Process
from pandas import DataFrame as df


def df_merge(df_list, on, how='left', fill_na=None):
    main_df = df_list.pop(0)
    for _df in df_list:
        main_df = pd.merge(main_df, _df, on=on, how=how)
    if fill_na is not None:
        main_df = main_df.fillna(fill_na)
    return main_df


def user_date_id(date, user_id):
    user_id = int(user_id)
    date = dt.datetime.strptime(str(date).split(' ')[0], '%Y-%m-%d')
    return user_id * pow(10, 8) + emdate.datetime_to_int(date, date_day=1)


def read_last_date(conn, table, date_col, where=None, date_format='stamp'):
    if where:
        _max_date = pd.read_sql(
            "select max({date}) md from {table} where {where}".format(date=date_col, table=table, where=where),
            conn
        )
    _max_date = pd.read_sql("select max({date}) md from {table}".format(date=date_col, table=table), conn)
    if date_format == 'datetime':
        return 1
    if date_format == 'date':
        return 2


def process_tool(s_num, e_num, ways, func, *args):
    process_list = []
    queue = cut_list(s_num, e_num, ways)
    for _one in queue:
        for step in _one:
            if step:
                process_list.append(Process(target=func, args=(*args, step,)))
        for _ in process_list:
            # time.sleep(1)
            _.start()
        for t in process_list:
            t.join()
        process_list.clear()


def thread_work(func, *args, tars, process_num=8, interval=0.03, step=None):
    pool = []
    while tars:
        if len(threading.enumerate()) <= process_num:
            _one = tars.pop(0)
            if step:
                pool.append(threading.Thread(target=func, args=(*args, _one)))
            else:
                pool.append(threading.Thread(target=func, args=(*args, )))
            pool[-1].start()
        time.sleep(interval)
    for t in pool:
        t.join()


def thread_work_kwargs(func, tars, process_num=8, interval=0.03, step=None, **kwarg):
    pool = []
    kwarg_dict = kwarg
    while tars:
        if len(threading.enumerate()) <= process_num:
            _one = tars.pop(0)
            if step:
                kwarg_dict.update({'num': _one})
                pool.append(threading.Thread(target=func, kwargs=kwarg_dict))
            else:
                pool.append(threading.Thread(target=func, kwargs=kwarg_dict))
            pool[-1].start()
        time.sleep(interval)
    for t in pool:
        t.join()


def thread_tool(s_num, e_num, ways, func, *args):
    threads = []
    queue = cut_list(s_num, e_num, ways)
    for _one in queue:
        for step in _one:
            if step:
                threads.append(threading.Thread(target=func, args=(*args, step,)))
        for _ in threads:
            time.sleep(0.1)
            _.start()
        for t in threads:
            t.join()
        threads.clear()


def cut_list(s_num, e_num, ways):
    _num = e_num - s_num + 1
    pool, surplus = _num // ways, _num % ways
    queue = []
    for _l in range(pool):
        _ladder = _l * ways
        _ = []
        for _n in range(ways):
            step = _ladder + _n + s_num
            _.append(step)
        queue.append(_)
        if _l == pool - 1:
            _ = []
            for _e in range(surplus):
                step = _ladder + _e + s_num + ways
                _.append(step)
            queue.append(_)
    return queue


def pad_col(_df, col_list=None, fill=0):
    if not col_list:
        col_list = [str(n+1) for n in range(30)]
    if isinstance(col_list, list):
        df_col = list(_df.columns)
        pad_list = [_ for _ in col_list if _ not in df_col]
        _df[pad_list] = fill
    return _df


def col_name_splicing(_df, col_list, new_col='union_col', symbol='_'):
    _col_left = col_list.pop(0)
    _col_right = col_list.pop(0)
    _df[new_col] = _df.apply(lambda x: _name_splicing(x[_col_left], symbol, x[_col_right]), axis=1)
    while col_list:
        _col_right = col_list.pop(0)
        _df[new_col] = _df.apply(lambda x: _name_splicing(x[new_col], symbol, x[_col_right]), axis=1)
    return _df
        
        
def _name_splicing(_left, symbol, _right):
    return str(_left) + str(symbol) + str(_right)


def list_name_splicing(col_list, symbol='_'):
    _col_left = col_list.pop(0)
    _col_right = col_list.pop(0)
    re_name = _name_splicing(_col_left, symbol, _col_right)
    while col_list:
        _col_right = col_list.pop(0)
        re_name = _name_splicing(_col_left, symbol, _col_right)
    return re_name


def df_pivot_table(_df, columns, index, values, agg_func='sum', fill_na=0):
    _df[values] = _df[values].astype(float)
    date_pivot = _df.pivot_table(
        columns=columns,
        index=index,
        values=values,
        aggfunc=agg_func
    )
    df_val = df(date_pivot.values)
    df_col = []
    for _col in date_pivot.columns:
        df_col.append(list_name_splicing(list(_col)))
    df_val.columns = df_col
    index_val = df([_ for _ in date_pivot.index])
    index_val.columns = index
    pivot_table = pd.concat([index_val, df_val], axis=1)
    pivot_table = pivot_table.fillna(fill_na)
    return pivot_table


def df_sort_col(_df, col_list, except_col=None):
    _df_col = _df.columns
    _col = [_ for _ in col_list if _ in _df_col]
    _df_col = list(set(_df_col).difference(set(_col)))
    if except_col:
        _col = _col + _df_col
    return _df[_col]

