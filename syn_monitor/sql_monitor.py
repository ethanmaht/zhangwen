

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
