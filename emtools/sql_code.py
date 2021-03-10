
# 日别-书-消耗看点
sql_kan_book_day = """
SELECT book_id,sum(kandian) kd,sum(free_kandian) fkd,
    date(from_unixtime(createtime)) createdate
FROM cps_shard_103.consume
where createtime >= unix_timestamp('{date}')
group by book_id,createdate
"""

sql_create_table = """
CREATE TABLE IF NOT EXISTS {db_name}.`{table_name}`(
   `{key_name}` BIGINT(20) UNSIGNED AUTO_INCREMENT,
   PRIMARY KEY ( `{key_name}` )
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""

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

sql_retain_date_day = """
select * 
from {db}.{tab}
where date_day = '{date}'
"""

sql_retain_date_day_30 = """
select user_id,date_day
from {db}.{tab}
where date_day >= '{s_date}' and date_day < '{e_date}'
"""

sql_read_last_date = """
SELECT MAX({dtype}) md FROM {db}.{tab}
"""

sql_first_order_time = """
select user_id,min(createtime) first_time from cps_user_{_num}.orders
where state = 1
group by user_id
"""

sql_order_info = """
select user_id,createtime,updatetime,state,type,book_id,chapter_id,admin_id,referral_id_permanent,
    money,money_benefit,benefit,kandian,free_kandian
where updatetime >= unix_timestamp('{date}')
FROM cps_user_{_num}.orders;
"""

sql_user_info = """
select id user_id,createtime,channel_id admin_id,sex,country,
    province,city,isp,referral_id,referral_id_permanent,ext 
where createtime >= unix_timestamp('{date}')
FROM cps_user_{_num}.user;
"""

sql_dict_total_admin = """
SELECT a.id,a.nickname,a.createtime,c.nickname business_name,c.id business_id 
FROM cps.admin a
    left join cps.admin_extend b on a.id = b.admin_id
    left join cps.admin c on b.create_by = c.id;
"""

sql_dict_total_book = """
SELECT id,book_category_id,name,real_read_num,author,state,sex,price,is_finish,free_chapter_num,first_chapter_id,
    last_chapter_id,read_num,is_cp,cp_name,real_read_num,book_recharge,booktag 
FROM cps.book;
"""

sql_dict_update_referral = """
SELECT id,book_id,chapter_id,chapter_name,name,cost,type,uv,follow,unfollow_num,net_follow_num,
    guide_chapter_idx,incr_num,money,orders_num,createtime,updatetime,state 
FROM cps.referral 
where updatetime >= unix_timestamp('{date}');
"""
