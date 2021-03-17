from emtools import emdate
from pandas import DataFrame as df


v = [_ for _ in range(10021)]
a = df({'key': v, 'val': v})

a = a.fillna(0)

# print(a)


def make_inert_sql(db_name, table, _data):
    _col = _data['columns']
    _len = len(_col)
    _col = tuple(_col)
    _col = str(_col).replace("'", "`")
    _char = '({len})'.format(len='%s,' * _len)[:-2] + ')'
    _val = [tuple(_) for _ in _data['data']]
    return f"replace INTO {db_name}.`{table}` {_col} VALUES {_char}", _val


def insert_to_data(write_data, db_name, table):
    _data = write_data.to_dict(orient='split')
    _sql, _val = make_inert_sql(db_name, table, _data)
    subsection_insert(_val)
    # print(_sql, _val)


def subsection_insert(_val, sub=1000):
    _s, _e, _top = 0, sub, len(_val)
    while _e < _top:
        val_sub = _val[_s: _e]
        print(val_sub)
        _s += sub
        _e += sub
    val_sub = _val[_s: _top]
    print(val_sub)


insert_to_data(a, 'db_name', 'tab_name')
