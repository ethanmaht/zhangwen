import pandas as pd


sql_create_table = """
CREATE TABLE IF NOT EXISTS {db_name}.`{table_name}`(
   `{key_name}` BIGINT(20) UNSIGNED AUTO_INCREMENT,
   PRIMARY KEY ( `{key_name}` )
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""


def insert_err(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except BaseException as e:
            print("[ERROR] func name <{name}> error reason is:".format(name=func.__name__), e)
    return wrapper


def chick_col(conn, db_name, table, _col, special):
    table_col = pd.read_sql(
        'select * from {db}.{tab} limit 1'.format(db=db_name, tab=table), conn
    ).columns.tolist()
    _add_cols = list(set(_col).difference(set(table_col)))
    if _add_cols:
        add_col(conn, db_name, table, _add_cols, special)


def add_col(conn, db_name, table, cols, special=None):
    cursor = conn.cursor()
    col_type = 'varchar(30)'
    if not special:
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


def make_inert_sql(db_name, table, _data, is_replace=None):
    if is_replace:
        method = 'replace'
    else:
        method = 'insert'
    _col = _data['columns']
    _len = len(_col)
    _col = tuple(_col)
    _col = str(_col).replace("'", "`")
    _char = '({len})'.format(len='%s,' * _len)[:-2] + ')'
    val = [tuple(_) for _ in _data['data']]
    sql = "{method} INTO {db_name}.`{tab}` {col} VALUES {char}".format(
        method=method, db_name=db_name, tab=table, col=_col, char=_char
    )
    return sql, val


@insert_err
def insert_to_data(
        write_data,  # 写入的数据
        conn,  # 数据库链接
        db_name,  # 数据库名称
        table,   # 表名
        force_create_table=False,  # 是否强制创建表
        key_name='id',  # 主键名称 可选 控制 create_table 方法
        force_create_col=False,  # 是否强制添加列
        **kwargs
):
    if force_create_table:
        create_table(conn, db_name, table, key_name)
        force_create_col = True

    _data = write_data.to_dict(orient='split')
    _col = _data['columns']

    if force_create_col:
        chick_col(conn, db_name, table, _col, **kwargs)

    _sql, _val = make_inert_sql(db_name, table, _data, **kwargs)

    cursor = conn.cursor()
    cursor.executemany(_sql, _val)
    conn.commit()


@insert_err
def subsection_insert_to_data(
        write_data, conn, db_name, table,
        sub=1000,  # 分片切入每片大小
        force_create_table=False,
        key_name='id',
        force_create_col=False,
        **kwargs
):
    if force_create_table:
        create_table(conn, db_name, table, key_name)
        force_create_col = True

    _data = write_data.to_dict(orient='split')
    _col = _data['columns']

    if force_create_col:
        chick_col(conn, db_name, table, _col, **kwargs)

    _sql, _val = make_inert_sql(db_name, table, _data, **kwargs)
    _subsection_insert(conn, _sql, _val, sub)


def _subsection_insert(conn, _sql, val, sub):
    _s, _e, _top = 0, sub, len(val)
    while _e < _top:
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


def create_table(conn, db_name, table_name, key_name):
    cursor = conn.cursor()
    cursor.execute(
        sql_create_table.format(
            db_name=db_name, table_name=table_name, key_name=key_name
        )
    )
    conn.commit()