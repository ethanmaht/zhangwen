# from sshtunnel import SSHTunnelForwarder
import yaml
import pymysql
from emtools import data_job
import pandas as pd
from emtools import sql_code
from emtools import emdate
import datetime as dt
import os
from emtools import currency_means as cm
import re
import difflib
from clickhouse_driver import Client


def read_db_host(file_path=None):
    if not file_path:
        file_path = os.path.split(os.path.realpath(__file__))[0] + '/config.yml'
    _file = open(file_path, 'r', encoding='utf-8')
    cont = _file.read()
    config = yaml.load(cont, Loader=yaml.FullLoader)
    return config


def read_db_config(conn_name):
    file_path = os.path.split(os.path.realpath(__file__))[0] + '/config.yml'
    _file = open(file_path, 'r', encoding='utf-8')
    cont = _file.read()
    config = yaml.load(cont, Loader=yaml.FullLoader)
    return config[conn_name]


def format_db_name(db_name, _num):
    return "{db_name}_{_num}".format(db_name=db_name, _num=_num)


def connect_database_vpn(base_name):
    file_path = os.path.split(os.path.realpath(__file__))[0] + '/config.yml'
    # _config = read_db_host('/home/datawork/emtools/config.yml')
    _config = read_db_host(file_path)
    _base = _config[base_name]
    conn = pymysql.connect(
        host=_base['host'], port=3306,
        user=_base['user'], passwd=_base['pw'],
    )
    return conn


def connect_database_host(host, user='cps_select', passwd='KU4CsBwrVKpmXt@4yk&LBDuI', port=3306):
    conn = pymysql.connect(
        host=host, port=port,
        user=user, passwd=passwd,
    )
    return conn


def connect_database_direct(host_dict, port=3306):
    conn = pymysql.connect(
        host=host_dict['host'], port=port,
        user=host_dict['user'], passwd=host_dict['pw'],
    )
    return conn


class DataBaseWork:

    def __init__(self):
        self.data_list = read_db_host(os.path.split(os.path.realpath(__file__))[0] + '/config.yml')['database_host']
        self.size = {}
        self.host = ''
        self.date = None
        self.process_num = 8
        self.date_sub = 3

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
        switch_job(db_name, self.host, size, self.process_num, self.date, self.date_sub)


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


def insert_data(write_data, conn, db_name, table, is_subsection=0, is_replace=False):
    cursor = conn.cursor()
    _data = write_data.to_dict(orient='split')
    val = _data['data']
    _col = _data['columns']
    if is_subsection:
        _s, _e, _top = 0, is_subsection, len(val)
        while _e < _top:
            val_sub = val[_s: _e]
            print('is insert data to db now:', _e, 'for ', _top)
            _sql = make_sub_mysql_inert_sql(db_name, table, _col, val_sub, _s, _e, is_replace=False)
            # print(_sql)
            _s += is_subsection
            _e += is_subsection
            cursor.execute(_sql)
            conn.commit()
        val_sub = val[_s: _e]
        print('is insert data to db now:', _e, 'for ', _top)
        _sql = make_sub_mysql_inert_sql(db_name, table, _col, val_sub, _s, _e, is_replace=False)
        cursor.execute(_sql)
        conn.commit()
    else:
        _sql = make_mysql_inert_sql(db_name, table, _data, is_replace)
        print('make_mysql_inert_sql:!!!')
        cursor.execute(_sql)
        conn.commit()


def subsection_insert_to_data(write_data, conn, db_name, table, key_name='id'):
    print('subsection_insert_to_data:', 1)
    create_table(conn, db_name, table, key_name)
    print('subsection_insert_to_data:', 2)
    _data = write_data.to_dict(orient='split')
    _col = _data['columns']
    print('subsection_insert_to_data:', 3)
    chick_col(conn, db_name, table, _col)
    print('subsection_insert_to_data:', 4)
    _sql, _val = make_inert_sql(db_name, table, _data)
    _subsection_insert(conn, _sql, _val)


def _subsection_insert(conn, _sql, val, sub=10000):
    _s, _e, _top = 0, sub, len(val)
    print('_subsection_insert', _e, len(val))
    while _e < _top:
        print('_subsection_insert', _e, len(val))
        val_sub = val[_s: _e]
        _data_executemany(conn, _sql, val_sub)
        print('is insert data to db now:', _e, 'for ', _top)
        _s += sub
        _e += sub
    val_sub = val[_s: _top]
    _data_executemany(conn, _sql, val_sub)
    print('is insert data to db now:', _e, 'for ', _top)


def _data_executemany(conn, _sql, _val):
    cursor = conn.cursor()
    cursor.executemany(_sql, _val)
    conn.commit()


def switch_job(db_name, conn_fig, size, process_num, date=None, date_sub=None):
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
        data_job.read_user_and_order(conn_fig, size, date, process_num, date_sub)
        data_job.read_data_user_day(conn_fig, size, date, process_num, date_sub)


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
    try:
        last_date = pd.read_sql(sql, conn)['md'][0]
    except:
        last_date = 0
    if is_list:
        _today = dt.datetime.now()
        return emdate.date_list(last_date, _today)
    return last_date


def delete_last_date(conn, db_name, tab_name, date_type_name, date, end_date=None, date_type='date'):
    if date_type == 'stamp':
        date = emdate.date_to_stamp(date)
        if end_date:
            end_date = emdate.date_to_stamp(end_date)
    del_sql = sql_code.sql_delete_last_date.format(type=date_type_name, db=db_name, tab=tab_name, date=date)
    cursor = conn.cursor()
    # print(sql_code.sql_delete_last_date.format(type=date_type_name, db=db_name, tab=tab_name, date=date))
    try:
        cursor.execute(del_sql)
    except Exception as e:
        print("Table {db}.{tab} doesn't exist".format(db=db_name, tab=tab_name))
    conn.commit()


def delete_last_date_num(
        conn, db_name, tab_name, date_type_name, date, num_type_name, num, date_type='date'
):
    if date_type == 'stamp':
        date = emdate.date_to_stamp(date)
    del_sql = sql_code.sql_delete_last_date_num.format(
        type=date_type_name, db=db_name, tab=tab_name, date=date, num_type=num_type_name, num=num
    )
    cursor = conn.cursor()
    try:
        cursor.execute(del_sql)
    except Exception as e:
        print("Table {db}.{tab} doesn't exist".format(db=db_name, tab=tab_name))
    conn.commit()


def delete_table_data(conn, db_name, tab_name):
    del_sql = sql_code.sql_delete_table_data.format(db=db_name, tab=tab_name)
    cursor = conn.cursor()
    try:
        cursor.execute(del_sql)
        conn.commit()
    except:
        print('delete_table_data: err', db_name, tab_name)


def syn_date_block_run(func, date, process_num, **kwargs):
    one_size = process_num // 4
    cm.thread_work_conn(
        func=func, date=date, one_size=one_size, **kwargs
    )


def _block_num_list(size):
    _s, _e = size['start'], size['end'] + 1
    return [_ for _ in range(_s, _e)]


def read_date_block(conn, sql, date_list, num):
    _re_data = []
    for _block in date_list:
        date_name, s_date, e_date = _block['date_name'], _block['s_date'], _block['e_date']
        one_num_data_block = pd.read_sql(
            sql.format(s_date=s_date, block=date_name, num=num), conn
        )
        _re_data.append(one_num_data_block)
    re_data = pd.concat(_re_data)
    return re_data


def data_sub(all_date_df, id_col, date_col):
    all_date_df.sort_values(by=[id_col, date_col], inplace=True)
    all_date_df.fillna(0, inplace=True)
    all_date_df[id_col] = all_date_df[id_col].astype(int)
    all_date_df['next_id'] = all_date_df[id_col].shift(1)
    all_date_df['next_date'] = all_date_df[date_col].shift(1)
    all_date_df['day_sub'] = all_date_df.apply(
        lambda x: _date_cat(x[id_col], x['next_id'], x[date_col], x['next_date']), axis=1
    )
    _all_date_df = all_date_df.loc[:, [id_col, date_col, 'day_sub']]
    _all_date_df.fillna(-1, inplace=True)
    return _all_date_df


def _date_cat(user_id, next_id, date_day, next_date):
    if not next_date:
        return -1
    if user_id != next_id:
        return -1
    return emdate.sub_date(next_date, date_day)


def write_click_date(write_data, client, db_name, table, step=0):
    _data = write_data.to_dict(orient='split')
    if step:
        _step_write_click_date(_data, client, db_name, table, step)
    else:
        _sql = make_click_inert_sql(db_name, table, _data)
        client.execute(_sql)
        print(f'Write date to click success: {len(_data)} row')


class OperateDataBase:

    def __init__(self, config_name):
        self.config_file_path = None
        self.config_file_name = '/db_config.yml'
        self.standard = ['mysql', 'mongodb', 'sqlsever', 'clickhouse', 'hive', 'hbase', ]
        self.type = 'mysql'
        self.config_name = config_name
        self.config = {}
        self.conn = self.db_connect()

    def __del__(self):
        if self.type == 'mysql':
            self.conn.close()

    def read_db_config(self):
        if not self.config_file_path:
            self.config_file_path = os.path.split(os.path.realpath(__file__))[0] + self.config_file_name

        _file = open(self.config_file_path, 'r', encoding='utf-8').read()

        return yaml.load(_file, Loader=yaml.FullLoader)[self.config_name]

    def db_connect(self):
        self.config = self.read_db_config()

        if 'type' in self.config:
            self.type = self.config['type']

        if str_correcting(self.type, self.standard) == 'mysql':
            return self.mysql_connect()

        if str_correcting(self.type, self.standard) == 'clickhouse':
            return self.click_connect()

    def mysql_connect(self):
        if 'port' in self.config:
            port = self.config['port']
        else:
            port = 3306

        if 'db_name' in self.config:
            return pymysql.connect(
                host=self.config['host'], port=port, user=self.config['user'], passwd=self.config['pw'],
                database=self.config['database']
            )

        return pymysql.connect(
            host=self.config['host'], port=port, user=self.config['user'], passwd=self.config['pw'],
        )

    def click_connect(self):
        if 'port' in self.config:
            return Client(
                host=self.config['host'], user=self.config['user'], password=self.config['pw'],
                port=self.config['port']
            )

        return Client(
            host=self.config['host'], user=self.config['user'], password=self.config['pw'],
        )

    def _create_table(self, db_name, table_name, key_name, **kwargs):
        if self.type in ['mysql', ]:
            _create_table_mysql(self.conn, db_name, table_name, key_name)
        if self.type in ['clickhouse', ]:
            _create_table_click(self.conn, db_name, table_name, **kwargs)

    def read_df_by_sql(self, sql):
        if self.type in ['mysql', ]:
            return pd.read_sql(sql, self.conn)
        if self.type in ['clickhouse', ]:
            return read_click_sql(sql, self.conn)

    def _delete_data(self, db_name, table_name, where=None):
        if self.type in ['mysql', ]:
            _delete_data_mysql(self.conn, db_name, table_name, where)
        if self.type in ['clickhouse', ]:
            _delete_data_click(self.conn, db_name, table_name, where)


def execute_click_sql(sql, client):
    client.execute(sql, columnar=True, with_column_types=True)
    print(f'Execute click sql <{sql}> success:')


def _create_table_mysql(conn, db_name, table_name, key_name):
    cursor = conn.cursor()

    cursor.execute(
        sql_create_table.format(
            db_name=db_name, table_name=table_name, key_name=key_name
        )
    )
    conn.commit()


def _write_sql_data(write_data, conn, db_name, table):
    _data = write_data.to_dict(orient='split')
    _col = _data['columns']
    _sql, _val = make_mysql_inert_sql(db_name, table, _data)
    cursor = conn.cursor()
    cursor.executemany(_sql, _val)
    conn.commit()


def _delete_data_mysql(conn, db_name, table_name, where=None):
    cursor = conn.cursor()

    sql = sql_delete_data.format(db_name=db_name, table_name=table_name)
    if where:
        sql += f' {where}'

    cursor.execute(sql)
    conn.commit()


def _create_table_click(conn, db_name, table_name, engine, col, partition=None, order=None, ):
    sql = click_create_table.format(
        db_name=db_name, table_name=table_name, col=col, engine=engine
    )

    if partition:
        sql += f'\nPARTITION BY {partition}'

    if order:
        sql += f'\nORDER BY {order}'

    conn.execute(sql, columnar=True, with_column_types=True)


def _write_click_date(write_data, client, db_name, table, step=0):
    _data = write_data.to_dict(orient='split')
    if step:
        _step_write_click_date(_data, client, db_name, table, step)
    else:
        _sql = make_click_inert_sql(db_name, table, _data)
        client.execute(_sql)
        print(f'Write date to click success: {len(_data)} row')


def _delete_data_click(conn, db_name, table_name, where=None):
    sql = click_delete_data.format(
        db_name=db_name, table_name=table_name
    )
    if where:
        sql += f' {where}'
    else:
        print('[ERR]: clickhouse delete data must have <where>!')
    conn.execute(sql, columnar=True, with_column_types=True)


def make_click_inert_sql(db_name, table, _data):
    _col = _data['columns']
    _len = len(_col)
    _col = tuple(_col)
    _col = str(_col).replace("'", "`")
    _val = str([tuple(_) for _ in _data['data']])[1:-1]
    return f"INSERT INTO {db_name}.`{table}` {_col} VALUES {_val}"


def _step_write_click_date(write_data, client, db_name, table, step=0):
    _data, _col = write_data['data'], write_data['columns']
    _s, _e, _top = 0, step, len(_data)
    while _e < _top:
        val_sub = _data[_s: _e]
        _sql = make_click_inert_step_sql(db_name, table, val_sub, _col)
        client.execute(_sql)
        print('is insert data to db now:', _e, 'for ', _top)
        _s += step
        _e += step
    val_sub = _data[_s: _top]
    _sql = make_click_inert_step_sql(db_name, table, val_sub, _col)
    client.execute(_sql)
    print('is insert data to db now:', _e, 'for ', _top)


def make_click_inert_step_sql(db_name, table, _data, col):
    _col = col
    _len = len(_col)
    _col = tuple(_col)
    _col = str(_col).replace("'", "`")
    _val = str([tuple(_) for _ in _data])[1:-1]
    return f"INSERT INTO {db_name}.`{table}` {_col} VALUES {_val}"


def read_click_sql(sql, client):
    data, columns = client.execute(sql, columnar=True, with_column_types=True)
    df = pd.DataFrame({re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})
    return df


def make_mysql_inert_sql(db_name, table, _data, is_replace=False):
    _col = _data['columns']
    data_val = _data['data']
    _len = len(_col)
    _col = tuple(_col)
    _col = str(_col).replace("'", "`")
    _char = '({len})'.format(len='%s,' * _len)[:-2] + ')'
    _val = str([tuple(_) for _ in data_val])[1:-1]
    if is_replace:
        return f"replace INTO {db_name}.`{table}` {_col} VALUES {_char}", _val
    return f"INSERT INTO {db_name}.`{table}` {_col} VALUES {_val}"


def make_sub_mysql_inert_sql(db_name, table, _col, _data, _s, _e, is_replace=False):
    _col = tuple(_col)
    _col = str(_col).replace("'", "`")
    _val = str([tuple(_) for _ in _data])[1:-1]
    if is_replace:
        return f"replace INTO {db_name}.`{table}` {_col} VALUES {_val}"
    return f"INSERT INTO {db_name}.`{table}` {_col} VALUES {_val}"


def str_correcting(_str, standard_list: list):
    _str = _str.lower()

    if _str in standard_list:
        return _str

    _list, stand_list = [], []
    for _, stand in enumerate(standard_list):
        score = difflib.SequenceMatcher(None, _str, stand)
        if score.quick_ratio() > .75:
            _list.append(_)
            stand_list.append(stand)
    if _list:
        return standard_list[_list[stand_list.index(max(stand_list))]]
    return ''


sql_create_table = """
CREATE TABLE IF NOT EXISTS {db_name}.`{table_name}`(
   `{key_name}` BIGINT(20) UNSIGNED AUTO_INCREMENT,
   PRIMARY KEY ( `{key_name}` )
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""

click_create_table = """
CREATE TABLE {db_name}.{table_name} (
    {col}
) ENGINE = {engine}
PARTITION BY {}
ORDER BY join_day;
"""

sql_delete_data = """
delete from {db}.{table}
"""

click_delete_data = """
alter table {db}.{table} delete
"""
