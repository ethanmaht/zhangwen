import re
import json
import pandas as pd
import pymysql
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import os
from email.mime.multipart import MIMEMultipart
import time


"""充值条数查询"""
sql_order = """
select l.pay_type_name,sum(o.discount) 
from glory_userdata.b_order as o ,glory_userdata.b_trade_log as l 
where o.ctime >= "2021-04-01" and o.ctime < "2021-05-01" and o.id=l.inner_trade_no 
and channel_code REGEXP "dz|qy" and price not in (0.01,0.02,0.1,0.2)group by l.pay_type_name
"""

sql_order_one_day = """
select
time, count(*)
from

(SELECT
 DISTINCT user_id,
 DATE_FORMAT( ctime, "%y-%m-%d") as time,
                                    count(*)
FROM
glory_userdata.b_order
WHERE
ctime >= "2021-04-01"
AND
status_notify = 1
AND
price
NOT
IN(0.01, 0.02, 0.1, 0.2)
and channel_code
REGEXP
"dz|qy"
GROUP
BY
time, user_id) as p
GROUP
BY
time
"""

sql_re_order = """
select
time, count(*)
from

(SELECT
 DISTINCT user_id,
 DATE_FORMAT( ctime, "%y-%m-%d") as time,
                                    count(*)
FROM
glory_userdata.b_order
WHERE
ctime >= "2021-04-01"
AND
status_notify = 1
AND
price
NOT
IN(0.01, 0.02, 0.1, 0.2)
and channel_code
REGEXP
"dz|qy"
GROUP
BY
time, user_id
HAVING
count(*) > 1) as p
GROUP
BY
time
"""

sql_pay_order = """
SELECT
l.pay_type_name,
sum(o.discount)
FROM
glory_userdata.b_order AS o,
glory_userdata.b_trade_log AS l
WHERE
o.ctime >= "2021-04-01"
AND
o.id = l.inner_trade_no
AND
channel_code
REGEXP
"dz|qy"
AND
price
NOT
IN(0.01, 0.02, 0.1, 0.2)
GROUP
BY
l.pay_type_name
"""

sql_order_money = """
SELECT
sum(price), sum(discount)
FROM
glory_userdata.b_order
WHERE
ctime >= "2021-04-01"
AND
channel_code
REGEXP
"qy|dz"
AND
status_notify = 1
"""

sql_consume = """
SELECT
 DATE_FORMAT( ctime, '%y-%m-%d' ) AS '日期',
 sum( amount ) AS '真币',
 sum( award ) AS '赠币',
 sum( original_amount ) AS '累计订阅' 
FROM
 glory_sum.b_owch_consume_h 
WHERE
 channel_code REGEXP 'dz|qy' 
 AND ctime >= '2021-04-01' 
GROUP BY
 DATE_FORMAT( ctime, '%y-%m-%d' )
"""

sql_relation = """
select channel_code as '渠道包',sum(add_num) as '新增用户',sum(desk_num) as '加桌用户',
round(sum(desk_num)/sum(add_num)*100) 
as '加桌率',sum(recharge_num) as '充值用户',
round(sum(recharge_num)/sum(add_num)*100) as '充值率',sum(recharge_money) as '累计充值金额',
sum(new_recharge_money) as '新用户充值金额' 
from kyycps.referral_channel_relation 
where createtime<1619798400 and channel_code regexp 'qy|dz' 
group by channel_code order by sum(recharge_money) desc
"""


def read_date(conn, sql_poll, name):
    # name = [
    #     'sql_order', 'sql_order_one_day', 'sql_re_order',
    #     'sql_pay_order',
    #     'sql_order_money', 'sql_consume', 'sql_relation'
    # ]
    n = 0
    for _ in sql_poll:
        data = pd.read_sql(_, conn)
        print()
        _name = name[n]
        data.to_excel('{name}.xlsx'.format(name=_name), index=0)
        n += 1


def mail(file_list):
    mail_host = "smtp.yeah.net"  # 设置服务器
    mail_user = "ma65065079@yeah.net"  # 用户名
    mail_pass = "GHNHCZLVSNCVZOTV"  # 口令

    sender = 'ma65065079@yeah.net'
    receivers = ['chenmeng@zhangwen.cn']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

    message = MIMEMultipart()
    message['From'] = Header("ma65065079@yeah.net", 'utf-8')
    message['To'] = Header("aijia", 'utf-8')

    subject = '自动数据'
    message['Subject'] = Header(subject, 'utf-8')
    message.attach(MIMEText('自动数据'))
    for _ in file_list:
        atta(message, _)
    att1 = MIMEText(open('{name}.xlsx'.format(name='sql_order'), 'rb').read(), 'base64', 'utf-8')
    att1["Content-Type"] = 'application/octet-stream'
    att1["Content-Disposition"] = 'attachment; filename="test.txt"'
    message.attach(att1)
    smtpObj = smtplib.SMTP()
    smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
    smtpObj.login(mail_user, mail_pass)
    smtpObj.sendmail(sender, receivers, message.as_string())
    print("邮件发送成功")


def atta(message, file_name):
    print(os.path.split(os.path.realpath(__file__))[0] + '/{name}.xlsx'.format(name=file_name))
    att1 = MIMEText(
        open(
            os.path.split(os.path.realpath(__file__))[0] + '/{name}.xlsx'.format(name=file_name), 'rb'
        ).read(), 'base64', 'utf-8'
    )
    att1["Content-Type"] = 'application/octet-stream'
    att1["Content-Disposition"] = 'attachment; filename="{name}.xlsx"'.format(name=file_name)
    message.attach(att1)


if __name__ == '__main__':
    print('start run:')
    time.sleep(61200)
    name = [
        'sql_order', 'sql_order_one_day', 'sql_re_order',
        'sql_pay_order',
        'sql_order_money', 'sql_consume', 'sql_relation'
    ]
    conn_order = pymysql.connect(
        host='47.98.63.24', port=13306,
        user='dianzhong', passwd='Heiyan@123',
    )
    sql_poll = [
        sql_order, sql_order_one_day, sql_re_order,
        sql_pay_order,
        sql_order_money, sql_consume, sql_relation]
    read_date(conn_order, sql_poll, name)

    mail(name)
    # conn_2 = pymysql.connect(
    #     host='47.98.63.24', port=13306,
    #     user='dianzhong', passwd='Heiyan@123',
    # )

    # conn_order = pymysql.connect(
    #     host='pc-bp1t4p4awz411rxe0.rwlb.rds.aliyuncs.com', port=3306,
    #     user='quickapp', passwd='Quickapp@123',
    # )

