from sshtunnel import SSHTunnelForwarder
import yaml
import pymysql
from emtools import data_job
import pandas as pd
from pandas import DataFrame as df
from emtools import sql_code
import os


def read_db_host(file_path):
    _file = open(file_path, 'r', encoding='utf-8')
    cont = _file.read()
    config = yaml.load(cont, Loader=yaml.FullLoader)
    return config


def format_db_name(db_name, _num):
    return "{db_name}_{_num}".format(db_name=db_name, _num=_num)


def connect_database(host, ):
    server = SSHTunnelForwarder(
        ('123.56.72.6', 22),
        ssh_username='root',
        ssh_password='#X@VnR4bVk!yF1LI9k7Mxq7h',
        remote_bind_address=(host, 3306)  # 远程数据库的IP和端口
    )
    server.start()
    conn = pymysql.connect(
        host='127.0.0.1', port=server.local_bind_port,
        user='cps_select', passwd='KU4CsBwrVKpmXt@4yk&LBDuI'
    )
    print('**********************')
    return conn


def connect_database_vpn(base_name):
    _config = read_db_host(os.getcwd() + '/config.yml')
    _base = _config[base_name]
    conn = pymysql.connect(
        host=_base['host'], port=3306,
        user=_base['user'], passwd=_base['pw'],
    )
    print('****** connect success: {db_name} ******'.format(db_name=base_name))
    return conn


def connect_database_host(host, user='cps_select', passwd='KU4CsBwrVKpmXt@4yk&LBDuI', port=3306):
    conn = pymysql.connect(
        host=host, port=port,
        user=user, passwd=passwd,
    )
    return conn


class DataBaseWork:

    def __init__(self):
        self.data_list = read_db_host('config.yml')['database_host']
        self.size = {}
        self.host = ''

    def loop_all_database(self):
        for _db in self.data_list:
            data_base = _db['data_base']
            self.host = data_base['host']
            self.loop_size_table(data_base)

    def loop_size_table(self, data_base):
        _db = data_base['name']
        if 'size' in data_base.keys():
            self.no_size_conn(_db, data_base['size'])
        else:
            self.no_size_conn(_db)

    def loop_size_conn(self, base):
        size = self.size
        _s, _e = size['start'], size['end']
        while _e >= _s:
            db_name = format_db_name(base, _s)
            conn = connect_database(self.host, )
            switch_job(base, conn, size)
            conn.close()
            _s += 1

    def no_size_conn(self, db_name, size=None):
        # conn = connect_database(self.host, )
        switch_job(db_name, self.host, size)
        # conn.close()


def chick_col(conn, db_name, table, _col):
    table_col = pd.read_sql(
        'select * from {db}.{tab} limit 1'.format(db=db_name, tab=table), conn
    ).columns.tolist()
    _add_cols = list(set(_col).difference(set(table_col)))
    if _add_cols:
        add_col(conn, db_name, table, _add_cols)


def add_col(conn, db_name, table, cols):
    cursor = conn.cursor()
    for _ in cols:
        _sql = "ALTER TABLE {db}.`{tab}` ADD COLUMN `{col}` varchar(20)".format(db=db_name, tab=table, col=_)
        cursor.execute(_sql)
        conn.commit()


def make_inert_sql(db_name, table, _data):
    _col = _data['columns']
    _len = len(_col)
    _col = tuple(_col)
    _col = str(_col).replace("'", "`")
    _char = '({len})'.format(len='%s,' * _len)[:-2] + ')'
    _val = [tuple(_) for _ in _data['data']]
    return f"replace INTO {db_name}.`{table}` {_col} VALUES {_char}", _val


def insert_to_data(write_data, conn, db_name, table, method=None):
    create_table(conn, db_name, table, 'ud_id')
    _data = write_data.to_dict(orient='split')
    _col = _data['columns']
    chick_col(conn, db_name, table, _col)
    _sql, _val = make_inert_sql(db_name, table, _data)
    cursor = conn.cursor()
    cursor.executemany(_sql, _val)
    conn.commit()
    print('****** write success {table} ******'.format(table=table))
    # try:
    #     cursor.executemany(_sql, _val)
    #     print('****** write success {table} ******'.format(table=table))
    #     conn.commit()
    # except:
    #     conn.rollback()


def switch_job(db_name, conn_fig, size):
    """
    this place is to add work, that we want to run.
    Examples:
        if db_name == 'db_name_1':
            what you want to run
            what you want to run
        if db_name == 'db_name_2':
            what you want to run
            ......
    :db_name -> the database name for loop to in this time.
    :conn -> the databases connect.
    :return -> null run in this place
    """
    if db_name == 'shard':
        data_job.read_data_user_day(conn_fig, size)


def read_from_sql(sql, conn):
    with conn.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
    return result


def create_table(conn, db_name, table_name, key_name):
    cursor = conn.cursor()
    cursor.execute(
        sql_code.sql_create_table.format(
            db_name=db_name, table_name=table_name, key_name=key_name
        )
    )
    conn.commit()
    # print('****** create table success {db}-{tab} ******'.format(db=db_name, tab=table_name))


if __name__ == "__main__":
    a = DataBaseWork()
    a.loop_all_database()
    # a = df({'ke': [1, 2], 'v': ['v1', 'v2']})
    # connect = connect_database_vpn('datamarket')
    # insert_to_data(a, connect, 'test')
    # connect.close()

