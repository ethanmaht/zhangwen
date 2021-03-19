import logging
from emtools import currency_means as cm
import threading
import time


def _test():
    time.sleep(5)
    print('***********')

conn_split = [128, 256, 334, 512]

conn_poll = []
_s = 0
split_plan = {}
one_size = 2
for _ in conn_split:
    conn_poll.append([_ for _ in range(_s, _)])
    _s = _
conn_num = len(conn_poll)
for t in range(conn_num):
    split_plan.update({'run_{num}'.format(num=t): [], 'poll_{num}'.format(num=t): conn_poll[t]})

# print(len(split_plan), split_plan)

surplus_num = 1
while surplus_num:
    surplus_num = 0
    for _con in range(conn_num):
        _run = split_plan['run_{num}'.format(num=_con)]
        _poll = split_plan['poll_{num}'.format(num=_con)]
        # for _t in _run:
        #     if _t.isAlive():
        if len(_run) < one_size:
            print(_poll[0])
            _num = _poll.pop(0)
            _run.append(threading.Thread(target=_test, kwargs={}))
            # t_pool.append(threading.Thread(target=_test, kwargs={}))
            _run[-1].start()
        surplus_num += len(_poll)


