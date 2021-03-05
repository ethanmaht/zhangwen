import pandas as pd
from pandas import DataFrame as df
from emtools import sql_code
import datetime as dt
from emtools import emdate


def retain_date_day(conn, date, s_num, e_num):
    date_dict = emdate.date_num_dict(date, 30)
    frist_day, last_day = list(date_dict.keys())[0], list(date_dict.keys())[-1]
    one_day = pd.read_sql(
        sql_code.sql_retain_date_day.format(date=date, s_num=s_num, e_num=e_num),
        conn
    )
    day_30 = pd.read_sql(
        sql_code.sql_retain_date_day_30.format(s_date=frist_day, e_date=last_day, s_num=s_num, e_num=e_num),
        conn
    )
    day_30['_'] = 1
    day_30 = day_30.pivot_table(index='user_id', columns='date_day', values='_', fill_value=0)
    one_day = pd.merge(one_day, day_30, on='user_id', how='left')
    one_day.rename(columns=date_dict, inplace=True)
    return one_day


a = df(
    {
        'user_id': [1, 3, 4, 5, 6, 8, 8],
        'date_day': ['2021-01-01', '2021-01-02', '2021-01-03', '2021-01-04', '2021-01-05', '2021-01-06', '2021-01-07']
    }
)
a['_'] = 1
print(a.pivot_table(index='user_id', columns='date_day', values='_', fill_value=0))
