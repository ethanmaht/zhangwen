import random, time, queue
from multiprocessing.managers import BaseManager
import numpy as np
from scipy.optimize import curve_fit
# import matplotlib.pyplot as plt
from pandas import DataFrame as df
from emtools import emdate
import pylab as pl
import math
from clickhouse_driver import Client
import pandas as pd
import re

d = {
    'k': [0, 0, 1, 1],
    'k2': [3, 3, 5, 6],
    'v1': [5, 6, 7, 8],
    'v2': ['a', 'b', 'c', 'd'],
    'd1': ['2021-05-01', '2021-05-01', '2021-05-01', '2021-05-02'],
    'd2': ['2021-05-03', '2021-05-05', '2021-05-06', '2021-06-02']
}
a = df(d)
