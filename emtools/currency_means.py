import pandas as pd
import datetime as dt
from emtools import emdate
import threading
import time
from multiprocessing import Process


def df_merge(df_list, on, how='left', fill_na=None):
    main_df = df_list.pop(0)
    for _df in df_list:
        main_df = pd.merge(main_df, _df, on=on, how=how)
    if fill_na is not None:
        main_df = main_df.fillna(fill_na)
    return main_df


def user_date_id(date, user_id):
    if not isinstance(user_id, int):
        user_id = int(user_id)
    if isinstance(date, str):
        date = dt.datetime.strptime(date.split(' ')[0], '%Y-%m-%d')
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
