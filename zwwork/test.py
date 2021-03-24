import logging
from emtools import currency_means as cm
import threading
import time
from emtools import read_database
from algorithm import retained
from pandas import DataFrame as df


# read_config = {'host': 'datamarket'}
# write_config = {'db_name': 'market_read', 'tab_name': 'test', 'date_col': 'date_day'}
# num = 0
# s_date = '2020-10-01'
# retained.retained_three_index_by_user(read_config, write_config, num, s_date=None)


a = df({'key': [1, 2, 3, 3], 'v': [1, 2, 3, 3], 'c': [1, 2, 3, 3]})
# a = a.groupby(by=['key', 'v']).count()
a.loc[:, ['iii']] = 1
print(a)
