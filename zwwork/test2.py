from zwwork import test
from emtools import read_database
import time
import os
import logging
from emtools import currency_means as cm


_path = cm.get_root_abs_path('datawork', add_self=1) + 'logs/syn_data.log'
print(_path)
