import os
from pandas import DataFrame as df
from algorithm import retained
from emtools import currency_means as cm
import time


a = df({'key': ['1', '2'], 'val': [3, None], 'test': [3, None]})
# a = a.fillna(0)
# print(a)


def test(_num):
    time.sleep(2)
    print(_num)


if __name__ == '__main__':
    a = a.rename(columns={'s': 'b'})
    print(a)
    # x = [1, 2, 3]
    # y = [2, 4]
    #
    # print(x + y)
