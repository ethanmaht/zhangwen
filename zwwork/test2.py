import threading
import time
from emtools import emdate


tar = [_ for _ in range(100)]


def test(a, b):
    time.sleep(3)
    print('thread: ', a, b)


def thread_work(func, *args, tars, process_num=8, interval=0.03, step=None):
    while tars:
        pool = []
        if len(threading.enumerate()) <= process_num:
            _one = tars.pop(0)
            if step:
                pool.append(threading.Thread(target=func, args=(*args, _one)))
            else:
                pool.append(threading.Thread(target=func, args=(*args, )))
            for _ in pool:
                _.start()
        time.sleep(interval)


# thread_work(test, 1, tars=tar, step=1)
# tars = [_ for _ in range(512)]
# print(tars)
# print('{a}'.format(a=1))
