
# 日别-书-消耗看点
sql_kan_book_day = """
SELECT book_id,sum(kandian) kd,sum(free_kandian) fkd,
    date(from_unixtime(createtime)) createdate
FROM cps_shard_103.consume
where createtime >= unix_timestamp('{date}')
group by book_id,createdate
"""

# 日别-用户-消费 1604160000 = 2020-11-01
sql_user_consume_day = """
SELECT user_id,date(from_unixtime(createtime)) date_day,sum(kandian) kd,
    sum(free_kandian) fd,count(*) chapters 
FROM cps_shard_{_num}.consume 
where createtime >= unix_timestamp('{date}')
group by user_id,date_day;
"""

sql_user_logon_day = """
SELECT id user_id,date(from_unixtime(createtime)) date_day,count(*) logon 
FROM cps_user_{_num}.user
where createtime >= unix_timestamp('{date}')
group by user_id,date_day;
"""

sql_user_sign_day = """
SELECT uid user_id,sum(kandian) sign_kd,count(*) signs,date(from_unixtime(createtime)) date_day
FROM cps_shard_{_num}.sign
where createtime >= unix_timestamp('{date}')
group by user_id,createdate;
"""

sql_user_order_day = """
SELECT user_id,date(from_unixtime(createtime)) date_day,sum(state) order_success,
    sum(if(state=0,1,0)) order_fail,
    sum(if(state=1,money,0)) money,sum(if(state=1,money_benefit,0)) money_benefit,
    sum(if(state=1,kandian,0)) order_kd,sum(if(state=1,free_kandian,0)) order_fd,
    sum(if(type=1 & state=1, 1, 0)) bays,sum(if(type=2 & state=1,1,0)) vips
FROM cps_user_{_num}.orders 
where createtime >= unix_timestamp('{date}')
group by user_id,date_day;
"""
