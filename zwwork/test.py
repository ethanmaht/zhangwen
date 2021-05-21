from pandas import DataFrame as df
import numpy as np
import pymysql
import pandas as pd
from emtools import read_database as rd
import json
import pyecharts
from typing import Dict
import copy
import datetime as dt


d = df({'a': ['2021-05-01', '2021-05-02', '2021-05-03']})

print(d)
d['b'] = pd.to_datetime(d['a'])
print(d.dtypes)



