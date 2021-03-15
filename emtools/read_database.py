# from sshtunnel import SSHTunnelForwarder
import yaml
import pymysql
from emtools import data_job
import pandas as pd
from emtools import sql_code
from emtools import emdate
import datetime as dt
import os


def read_db_host(file_path):
    file_path = os.path.split(os.path.realpath(__file__))[0] + '/config.yml'
    # print('**************', file_path)
    _file = open(file_path, 'r', encoding='utf-8')
    cont = _file.read()
    config = yaml.load(cont, Loader=yaml.FullLoader)
    return config


def format_db_name(db_name, _num):
    return "{db_name}_{_num}".format(db_name=db_name, _num=_num)


# def connect_database(host, ):
#     server = SSHTunnelForwarder(
#         ('123.56.72.6', 22),
#         ssh_username='root',
#         ssh_password='#X@VnR4bVk!yF1LI9k7Mxq7h',
#         remote_bind_address=(host, 3306)  # 远程数据库的IP和端口
#     )
#     server.start()
#     conn = pymysql.connect(
#         host='127.0.0.1', port=server.local_bind_port,
#         user='cps_select', passwd='KU4CsBwrVKpmXt@4yk&LBDuI'
#     )
#     return conn


def connect_database_vpn(base_name):
    _config = read_db_host('/home/datawork/emtools/config.yml')
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
        self.data_list = read_db_host('/home/datawork/emtools/config.yml')['database_host']
        self.size = {}
        self.host = ''
        self.date = None
        self.process_num = 16

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

    def no_size_conn(self, db_name, size=None):
        # conn = connect_database(self.host, )
        switch_job(db_name, self.host, size, self.process_num, self.date)
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
    col_type = 'varchar(30)'
    special = {
        'province': 'varchar(50)',
        'ext': 'TEXT',
    }
    for _ in cols:
        if _ in special.keys():
            col_type = special[_]
        _sql = "ALTER TABLE {db}.`{tab}` ADD COLUMN `{col}` {type}".format(db=db_name, tab=table, col=_, type=col_type)
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


def insert_to_data(write_data, conn, db_name, table, key_name='id'):
    create_table(conn, db_name, table, key_name)
    _data = write_data.to_dict(orient='split')
    _col = _data['columns']
    chick_col(conn, db_name, table, _col)
    _sql, _val = make_inert_sql(db_name, table, _data)
    cursor = conn.cursor()
    cursor.executemany(_sql, _val)
    conn.commit()


def switch_job(db_name, conn_fig, size, process_num, date=None):
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
    if db_name == 'happy_seven':
        data_job.read_dict_table(conn_fig, 'datamarket', date)
    if db_name == 'shard':
        data_job.read_user_and_order(conn_fig, size, date, process_num)
        data_job.read_data_user_day(conn_fig, size, date, process_num)


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


def read_last_date(conn, db_name, tab_name, date_type_name, is_list=None):
    sql = sql_code.sql_read_last_date.format(dtype=date_type_name, db=db_name, tab=tab_name)
    last_date = pd.read_sql(sql, conn)['md'][0]
    if is_list:
        _today = dt.datetime.now()
        return emdate.date_list(last_date, _today)
    return last_date


def delete_last_date(conn, db_name, tab_name, date_type_name, date):
    del_sql = sql_code.sql_delete_last_date.format(type=date_type_name, db=db_name, tab=tab_name, date=date)
    print(del_sql)
    cursor = conn.cursor()
    cursor.execute(del_sql)
    conn.commit()


def delete_table_data(conn, db_name, tab_name):
    del_sql = sql_code.sql_delete_table_data.format(db=db_name, tab=tab_name)
    cursor = conn.cursor()
    try:
        cursor.execute(del_sql)
        conn.commit()
    except:
        print('delete_table_data: err', db_name, tab_name)
