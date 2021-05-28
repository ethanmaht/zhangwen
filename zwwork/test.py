from pandas import DataFrame as df
import pandas
from pyhive import hive
import string


# d = df({'a': ['2021-05-01', '2021-05-02', '2021-05-03']})
#
# conn = hive.Connection(host='192.168.100.52', port=10000, database='dwd_qy_db')
# print('conn ok')
# try:
#     a = pandas.read_sql('SELECT * FROM dwd_qy_profile_cpsshardn_consume_t limit 10', conn)
#     print(a)
# except:
#     print('read_sql err')
# cursor = conn.cursor()
# print('cursor ok')

t = (('短信-APP',), ('微信-QQ-APP',), ('电话-微信-APP',), ('短信-APP',), ('街边',), ('街边',), ('微信-APP',), ('QQ-小程序',), ('信息泄露',), ('电话-QQ-微信-淘宝',), ('微信',), ('微信',), ('信息泄露',), (None,), ('QQ-微信-支付宝',))


class WordRate:

    def __init__(self, word_list, sign="-"):
        self.word_list = word_list
        self.sign = sign
        self.rate_dick = {}

    def cut_word(self, _str):
        if _str:
            return _str.split(self.sign, _str.count(self.sign))

    def get_word_list(self):
        re_list = []
        for _ in self.word_list:
            words = _[0]
            re_list.append(self.cut_word(words))
        return re_list

    def word_rate(self):
        _list = self.get_word_list()
        for _one in _list:
            diff_list = self.difference_list(_one)
            if diff_list:
                self.update_diff_word(diff_list)
            self.rate_dick_count_num(_one)
        return self.rate_dick

    def update_diff_word(self, diff_list):
        for _ in diff_list:
            self.rate_dick.update({_: 0})

    def difference_list(self, list_tar, list_poll=None) -> list:
        if not list_poll:
            list_poll = self.rate_dick.keys()
        if not list_tar:
            list_tar = []
        return list(set(list_tar).difference(set(list_poll)))

    def rate_dick_count_num(self, _word_list):
        if _word_list:
            for _ in _word_list:
                self.rate_dick[_] = self.rate_dick[_] + 1

    def format_rate_data(self, types):
        if types == 'list':
            return list(self.rate_dick.items())
        return self.rate_dick


a = WordRate(t)
a.word_rate()
b = a.format_rate_data('list')
print(b)
