import random, time, queue
from multiprocessing.managers import BaseManager
import numpy as np
from scipy.optimize import curve_fit
# import matplotlib.pyplot as plt
from pandas import DataFrame as df
from emtools import emdate
import pylab as pl
import math
from clickhouse_driver import Client
import pandas as pd
import re

# d = {
#     'k': [0, 0, 1, 1],
#     'k2': [3, 3, 5, 6],
#     'v1': [5, 6, 7, 8],
#     'v2': ['a', 'b', 'c', 'd'],
#     'd1': ['2021-05-01', '2021-05-01', '2021-05-01', '2021-05-02'],
#     'd2': ['2021-05-03', '2021-05-05', '2021-05-06', '2021-06-02']
# }
# a = df(d)
#
#

# client = Client(host='121.40.53.178', user='testuser', password='1a2s3d4f', database='heiyan')
# print('client ok')
# sql = """
# CREATE TABLE heiyan.mysql_rock_wings_writing_chapter_pt_day(
#     ip String,
#     book_id String,
#     content_id String,
#     i_name String,
#     free String,
#     words Int8,
#     create_time DateTime,
#     sequence Int8,
#     status String,
#     day String
# ) ENGINE = MergeTree()
# ORDER BY ip;
# """

# sql = """
# INSERT INTO mysql_rock_wings_writing_chapter_pt_day(
#     ip, book_id, content_id, create_time
# )
# VALUES
#     ('0', '0', '0', '2021-06-01')
# """

sql = """
select * from mysql_rock_wings_writing_chapter_pt_day
"""

