from pandas import DataFrame as df
import pymysql
import pandas as pd


def db_work(comm):
    cursor = comm.cursor()
    return cursor


def db_select(conn, sql):
    _df = pd.read_sql(sql, conn)
    return _df


def test_work(conn, name):
    sql = """
    select * from user limit 10
    """
    print(db_select(conn, sql))

