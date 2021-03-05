import time
import threading
from emtools import currency_means as cm
from multiprocessing import Process
from emtools import read_database as rd
import pymysql
import pandas as pd
import datetime as dt
import os
from pandas import DataFrame as df
from algorithm import retained


# a = df({'ke': [1, 2], 'v': ['v1', 'v2']})
def test(*aa):
    conn = connect_database_vpn()
    a = rd.read_from_sql('select * from cps_user_0.user limit 100', conn)
    conn.close()


def connect_database_vpn():
    conn = pymysql.connect(
        host='rr-2ze03zpy7yf536008.mysql.rds.aliyuncs.com', port=3306,
        user='cps_select', passwd='KU4CsBwrVKpmXt@4yk&LBDuI'
    )
    print('****** connect success: {db_name} ******')
    return conn
# currency_means.process_tool(1, 17, 2, test, 'aa', 'bb')


if __name__ == '__main__':
    # print(os.getcwd())
    # a = df({'ke': 1, 'v': 2}, index=[0])
    # print(a)
    a = retained.KeepTableDay()
    a.count_keep_table_day_run()

    # conns = connect_database_vpn()
    # t_s = dt.datetime.now()
    # cm.thread_tool(1, 15, 3, test,)
    # for i in range(100):
    #     a = rd.read_from_sql('select * from cps_user_0.user limit 100', conns)
    #     # a = pd.read_sql('select * from cps_user_0.user limit 100', conns)
    # t_1 = dt.datetime.now()
    # conns.close()
    # print(t_1 - t_s)



