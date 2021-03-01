import datetime as dt
from emtools import emdate


tt = dt.datetime(
    year=2021, month=3, day=1,
    hour=1, minute=13, second=21
)

a = emdate.datetime_format(tt, date_time=1, format_sisign="/")
print(a)

