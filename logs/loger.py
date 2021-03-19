import logging
from emtools import currency_means as cm


def logging_read(func):
    def wrapper(*args, **kwargs):
        try:
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
