import pandas as pd
import datetime as dt
from emtools import emdate


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
