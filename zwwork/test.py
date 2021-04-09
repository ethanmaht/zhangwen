import time
from pandas import DataFrame as df
from algorithm import retained
from es_worker import ec_market
from emtools import emdate


a = emdate.FunctionTimer()

time.sleep(1)
a.time_spot()
