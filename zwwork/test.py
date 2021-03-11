import os
from pandas import DataFrame as df
from algorithm import retained


# a = df({'key': [1, 2], 'val': [3, None]})
# a = a.fillna(0)
# print(a)


def test():
    print(__file__)
    print(os.path.split(os.path.realpath(__file__)))


if __name__ == '__main__':
    a = retained.RunCount(retained.count_order_logon_conversion, 'order_logon_conversion', 'order_day')
    # a.step_run()
    a.direct_run(retained.compress_order_logon_conversion)
