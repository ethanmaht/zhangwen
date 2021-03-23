import logging
from emtools import currency_means as cm
import threading
import time
from emtools import read_database
from algorithm import retained


read_config = {'host': 'datamarket'}
write_config = {'db_name': 'market_read', 'tab_name': 'test', 'date_col': 'date_day'}
num = 0
s_date = '2020-10-01'
retained.retained_three_index_by_user(read_config, write_config, num, s_date=None)
