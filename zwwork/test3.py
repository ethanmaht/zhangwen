import random, time, queue
from multiprocessing.managers import BaseManager
import numpy as np
from scipy.optimize import curve_fit
# import matplotlib.pyplot as plt
from pandas import DataFrame as df
import pylab as pl
import math


sql1 = """
SELECT admin_id AS '渠道id',sum( money ) as '累计充值金额',sum( money ) / count( DISTINCT ( user_id ) ) AS '人均充值金额'
FROM {new_file_name} 
where state=1 and deduct=0 and admin_id in {channel_name} and createtime>=1619798400 GROUP BY admin_id
"""

print(sql1.format(new_file_name=1, channel_name=2), '\n')

new_file_name = 11
channel_name = 21
sql2 = f"""
SELECT admin_id AS '渠道id',sum( money ) as '累计充值金额',sum( money ) / count( DISTINCT ( user_id ) ) AS '人均充值金额'
FROM {new_file_name} 
where state=1 and deduct=0 and admin_id in {channel_name} and createtime>=1619798400 GROUP BY admin_id
"""

print(sql2)
