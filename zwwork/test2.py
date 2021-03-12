from emtools import emdate
from pandas import DataFrame as df

a = df({'key': [1, 2], 'val': [3, None]})

a = a.fillna(0)

print(a)



