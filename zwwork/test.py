import datetime as dt
import time
from pandas import DataFrame as df


_df = df({'v_1': [1, 2, 3], 'v_2': [5, 2, 3]})
a = 2


def test(a, b):
    if not b:
        return 0
    return a / b


def divide(_df, x_left, x_right):
    _df['ok'] = _df.apply(lambda x: test(x[x_left], x[x_right]), axis=1)
    return _df


def test1():
    return 1


divide(_df=_df, x_left='', x_right='')
if isinstance(a, int):
    print(1)
