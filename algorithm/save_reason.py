# coding=utf8
import pandas as pd
from pandas import DataFrame as df
from emtools import sql_code
from emtools import currency_means as cm
from emtools import read_database as rd
from emtools import emdate
from algorithm import retained
import datetime as dt


def run_save_reason(_date):
    tar_date_list = emdate.date_list(_date, e_date=dt.datetime.now(), format_code='{Y}-{M}-{D}')
    for _day in tar_date_list:
        print('****** Start to run: {d} - count_keep_table_day ******'.format(d=_day))
        tars = [_ for _ in range(512)]
        cm.thread_work(one_day_run, _day, tars=tars, process_num=32, interval=0.03, step=1)


def one_day_run(date, num):
    print('======> is run run_save_reason to date:', date, ' num:', num)
    conn = rd.connect_database_host('172.16.0.248', 'root', 'Qiyue@123')
    day_pool = [2, 3, 7, 14, 30]
    e_day_list = emdate.date_list(date, num=day_pool, format_code='{Y}-{M}-{D}', direction=1)
    _data = []
    for _day in e_day_list:
        one_data = pd.read_sql(
            sql_code.analysis_reason_for_save.format(s_day=date, e_day=_day, tab_num=num),
            conn
        )
        one_data['save_day'] = day_pool.pop(0)
        _data.append(one_data)
    one_day_num = pd.concat(_data)
    one_day_num['date_day'] = date
    one_day_num = one_day_num.fillna(0)
    rd.insert_to_data(one_day_num, conn, 'market_read', 'save_reason')


def make_sample_date_sub_list():
    _sample = retained.make_sample_list(25, 511)

    return 0


def pull_date_sample_one_tab(conn, _sql, db_name, tab_name, s_date, num, e_date=None):
    if not e_date:
        e_date = emdate.datetime_format_code(dt.datetime.now())
    _sample_date = pd.read_sql(_sql.format(db=db_name, tab=tab_name, s_date=s_date, e_date=e_date, num=num), conn)
    return _sample_date


def one_user_date_sub(_df, date_id, date_type):
    # _df = df()
    _df.sort_values(by=[date_id, date_type], axis=0, inplace=True)
    _df['n_date'] = _df[date_type].shift(-1)
    _df['n_id'] = _df[date_id].shift(-1)
    _df = _df.loc[_df['n_id'] == _df[date_id], :]
    _df['_sub'] = _df.apply(lambda x: emdate.sub_date(x[date_type], x['n_date']), axis=1)
    print(_df)


if __name__ == '__main__':
    print(0)
    a = df({
        'id': [1, 1, 2, 2, 3, 3],
        'date': ['2021-01-01', '2021-02-01', '2021-01-01', '2021-02-01', '2021-01-01', '2021-02-01']
    })
    one_user_date_sub(a, 'id', 'date')
    # run_save_reason('2021-01-01')
