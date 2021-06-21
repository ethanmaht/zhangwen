import difflib

# a = {'a': 1}
# print('a' in a)
#
#
# x = 'abcdefg'
# y = 'afbcdefg'
#
# s = difflib.SequenceMatcher(None, x, y)
# print(s.quick_ratio())


test_list = ['mysql', 'mongodb', 'sqlsever', 'clickhouse', 'hive', 'hbase', ]


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


print(str_correcting('mongo_db', test_list))

a = 'adfsd'
a += 'vd'
print(a)
