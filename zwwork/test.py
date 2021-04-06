import time
from pandas import DataFrame as df
from algorithm import retained
from es_worker import ec_market


a = df({'k': [0, 1], 'v': ['2', '3']})

a['v'].astype(float)

print(a)


