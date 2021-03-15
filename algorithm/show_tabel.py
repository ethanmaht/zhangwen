# coding=utf8
import pandas as pd
from emtools import currency_means as cm
from emtools import read_database as rd
from algorithm import sql_show_tab as sql


def logon_admin_book_val(host, write_db, write_tab, date_col, date):
    conn = rd.connect_database_host(host['host'], host['user'], host['pw'])
    _date = pd.read_sql(sql.sql_logon_admin_book_val.format(date=date), conn)
    _date = cm.df_pivot_table(
        _date,
        columns=['box'],
        index=['商务', '公众号', '书名', '激活日期'],
        values=['激活', '首充_用户', '首充_金额', '复充_用户', '复充_金额', 'vip用户', 'vip金额']
    )
    _date = cm.df_sort_col(
        _date,
        ['商务', '公众号', '书名', '激活日期', '激活_首日',
         '首充_用户_首日', '首充_用户_次日', '首充_用户_三日', '首充_用户_四至七日', '首充_用户_七日以上',
         '首充_金额_首日', '首充_金额_次日', '首充_金额_三日', '首充_金额_四至七日', '首充_金额_七日以上',
         '复充_用户_首日', '复充_用户_次日', '复充_用户_三日', '复充_用户_四至七日', '复充_用户_七日以上',
         '复充_金额_首日', '复充_金额_次日', '复充_金额_三日', '复充_金额_四至七日', '复充_金额_七日以上',
         'vip用户_首日', 'vip用户_次日', 'vip用户_三日', 'vip用户_四至七日', 'vip用户_七日以上',
         'vip金额_首日', 'vip金额_次日', 'vip金额_三日', 'vip金额_四至七日', 'vip金额_七日以上']
    )
    rd.insert_to_data(_date, conn, write_db, write_tab)
    conn.close()
