import os
from pandas import DataFrame as df


a = df({'key': [1, 2], 'val': [3, None]})
a = a.fillna(0)
print(a)


def test():
    print(__file__)
    print(os.path.split(os.path.realpath(__file__)))
