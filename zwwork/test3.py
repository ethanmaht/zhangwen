import random, time, queue
from multiprocessing.managers import BaseManager
import numpy as np
from scipy.optimize import curve_fit
# import matplotlib.pyplot as plt
from pandas import DataFrame as df
from emtools import emdate
import pylab as pl
import math

d = {
    'k': [0, 0, 1, 1],
    'k2': [3, 3, 5, 6],
    'v1': [5, 6, 7, 8],
    'v2': [6, 7, 8, 9],
    'd1': ['2021-05-01', '2021-05-01', '2021-05-01', '2021-05-02'],
    'd2': ['2021-05-03', '2021-05-05', '2021-05-06', '2021-06-02']
}
a = df(d)
a[['k2', 'v1']] = a[['k2', 'v1']].astype(str)
print(a.dtypes)
# a['s'] = a[['d2', 'd1']].apply(lambda x: emdate.sub_date(x['d1'], x['d2']), axis=1)
# print(a)
#
#
# a = a.groupby(by=['k', 'k2']).sum().reset_index()
# print(a)
