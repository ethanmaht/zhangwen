import random, time, queue
from multiprocessing.managers import BaseManager
import numpy as np
from scipy.optimize import curve_fit
# import matplotlib.pyplot as plt
from pandas import DataFrame as df


city = df(
    {
        'city': ['北京', '天津', '上还']
    }
)

adr = df(
    {
        'address': ['北京市朝阳区', '天津市红灯区', '纽约市']
    }
)


def find_loop(address, _city: list, na_fill=''):
    for _ in _city:
        if _ in address:
            return _
    return na_fill


city_list = city['city'].to_list()
adr['city'] = adr['address'].apply(lambda x: find_loop(x, city_list))
print(adr)


#
# d = {
#     'k': [0, 0, 1, 1],
#     'k2': [3, 3, 5, 6],
#     'v1': [5, 6, 7, 8],
#     'v2': ['1月', '1月', '1月', '2月'],
#     'd1': ['2021-05-01', '2021-05-01', '2021_05_01', '2021/05/02'],
#     'd2': ['2021-05-03', '2021-05-05', '2021-05-06', '2021-06-02']
# }
# a = df(d)
#
# a['d1'] = a['d1'].apply(lambda x: str(x).replace('-', '').replace('_', '').replace('/', ''))
# print(a)
