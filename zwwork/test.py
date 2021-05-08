from pandas import DataFrame as df
import numpy as np
import pymysql
import pandas as pd
from emtools import read_database as rd
import json
import pyecharts


def df_columns_index_reset(_df):
    index_name, index_values = _df.index.name, _df.index.values
    re_df = df(_df.values)
    col_name, _one_col = [], None
    for _cols in _df.columns:
        for _ in _cols:
            if _one_col:
                _one_col = _one_col + '_' + _
            else:
                _one_col = _
        col_name.append(_one_col)
        _one_col = None
    re_df.columns = col_name
    re_df[index_name] = index_values
    return re_df


conn = rd.connect_database_vpn('datamarket')

sql = """
SELECT * from model.mid
"""

_d = pd.read_sql(sql, conn)

test = _d.to_dict(orient='records')
for _ in test:
    print(_)
    json1 = eval(_['active_sub'])
    print(json1)


