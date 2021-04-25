
sql_order_num = """
SELECT date(FROM_UNIXTIME({date_col})) date_day,
    count(*) order_times,count(DISTINCT user_id) order_users, 
    sum(money) order_money, sum(kandian / 100) kandian,{num} tab_num
from {db_tab}
where {date_col} >= UNIX_TIMESTAMP('{date}') and state = '1' and deduct = 0
GROUP BY date_day
"""

sql_user_num = """
SELECT date(FROM_UNIXTIME({date_col})) date_day,count(*) logon_user,{num} tab_num
from {db_tab}
where {date_col} >= UNIX_TIMESTAMP('{date}')
GROUP BY date_day
"""

sql_tab_data_num_by_date = """
SELECT date({date_col}) date_day,{num} tab_num,'{db_tab}' table_name,count(*) data_nums
from {db_tab}
where {date_col} >= '{date}'
GROUP BY date_day
"""

sql_tab_data_num_by_stamp = """
SELECT date(FROM_UNIXTIME({date_col})) date_day,{num} tab_num,'{db_tab}' table_name,count(*) data_nums
from {db_tab}
where {date_col} >= UNIX_TIMESTAMP('{date}')
GROUP BY date_day
"""


sql_comparison_admin_book_order_lift = """
SELECT order_day date_day,sum(order_times) order_times, sum(order_user) order_users, sum(logon_user) logon
from market_read.order_logon_conversion
where order_day >= '{date}' and order_day < '{e_date}'
GROUP BY date_day
"""

sql_comparison_admin_book_order_orders = """
SELECT date_day,sum(order_times) order_times, sum(order_users) order_users
from syn_monitor.monitor_orders_syn
where date_day >= '{date}' and date_day < '{e_date}'
GROUP BY date_day
"""

sql_comparison_admin_book_order_users = """
SELECT date_day,sum(logon_user) logon
from syn_monitor.monitor_user_syn
where date_day >= '{date}' and date_day < '{e_date}'
GROUP BY date_day
"""

sql_comparison_one_book = """
SELECT *,date(FROM_UNIXTIME(createtime)) create_date
from orders_log.orders_log_{num}
where createtime >= UNIX_TIMESTAMP('{date}') and createtime < UNIX_TIMESTAMP('{e_date}') and book_id = {bookid}
"""


sql_max_date = """
SELECT '{db_tab}' table_name,max({date_col}) update_time from {db_tab}
"""
