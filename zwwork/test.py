import os
from pandas import DataFrame as df
from algorithm import retained
from emtools import currency_means as cm


a = df({'key': ['1', '2'], 'val': [3, None], 'test': [3, None]})
# a = a.fillna(0)
# print(a)


def test():
    print(__file__)
    print(os.path.split(os.path.realpath(__file__)))


if __name__ == '__main__':
    a = cm.df_sort_col(a, ['test', 'key', 'xx'], except_col=1)
    print(a)
    # x = [1, 2, 3]
    # y = [2, 4]
    #
    # print(x + y)
