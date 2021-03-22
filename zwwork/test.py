import logging
from emtools import currency_means as cm
import threading
import time


def _test(a):
    time.sleep(2)
    print('***********', a)


conn_split = [128, 256, 334, 512]

# conn_poll = []
# _s = 0
# split_plan = {}
# one_size = 2
# for _ in conn_split:
#     conn_poll.append([_ for _ in range(_s, _)])
#     _s = _
# conn_num = len(conn_poll)
# for t in range(conn_num):
#     split_plan.update({'run_{num}'.format(num=t): [], 'poll_{num}'.format(num=t): conn_poll[t]})


def _make_split_plan_dict(conn_split, start_num=0):
    conn_poll = []
    split_plan = {}
    for _ in conn_split:
        conn_poll.append([_ for _ in range(start_num, _)])
        _s = _
    conn_num = len(conn_poll)
    for t in range(conn_num):
        split_plan.update({'run_{num}'.format(num=t): [], 'poll_{num}'.format(num=t): conn_poll[t]})
    return split_plan

# print(len(split_plan), split_plan)


def run_conn_split_plan(_split_list, one_size=2):
    _split_list = [128, 256, 334, 512]
    split_plan = _make_split_plan_dict([128, 256, 334, 512], start_num=0)
    surplus_num = 1
    while surplus_num:
        surplus_num = 0
        for _con in range(len(_split_list)):
            _run = split_plan['run_{num}'.format(num=_con)]
            _poll = split_plan['poll_{num}'.format(num=_con)]
            for _t in _run:
                if not _t.is_alive():
                    _run.remove(_t)
            if len(_run) < one_size:
                _num = _poll.pop(0)
                _run.append(threading.Thread(target=_test, args=(_num, )))
                _run[-1].start()
            surplus_num += len(_poll)
            time.sleep(0.03)


