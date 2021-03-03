import datetime as dt
from emtools import emdate
from pandas import DataFrame as df
from emtools import read_database


a = df({'ke': [1, 2], 'v': ['v1', 'v2']})


conn = read_database.connect_database_vpn('datamarket')
print(1)
read_database.insert_to_data(a, conn, 'test')

