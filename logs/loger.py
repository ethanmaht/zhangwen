import logging
from emtools import currency_means as cm
import os
import datetime as dt


def logging_read(func):
    def wrapper(*args, **kwargs):
        try:
            logging_start_up(func.__name__)
            return func(*args, **kwargs)
        except:
            _path = cm.get_root_abs_path('datawork', add_self=1) + 'logs/syn_data.log'
            logging.basicConfig(
                level=logging.DEBUG,
                filename=_path,
                datefmt='%Y/%m/%d %H:%M:%S',
                format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(module)s - %(message)s'
            )
            logging.error('Error', exc_info=True)
    return wrapper


def logging_start_up(table_name):
    _path = cm.get_root_abs_path('datawork', add_self=1) + 'logs/start.txt'
    _log = "[MissionStart]:mission name '{tab}'; time:{time}\n".format(tab=table_name, time=dt.datetime.now())
    f = open(_path, 'a')
    f.write(_log)
