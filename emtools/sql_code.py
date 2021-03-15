
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

sql_delete_last_date = """
delete from {db}.{tab} where {type} >= '{date}'
"""

sql_delete_table_data = """
delete from {db}.{tab}
"""

sql_first_order_time = """
select user_id,min(createtime) first_time from cps_user_{_num}.orders
where state > 0
group by user_id
"""

sql_order_info = """
select id,user_id,createtime,updatetime,state,type,book_id,chapter_id,admin_id,referral_id_permanent,
    money,money_benefit,benefit,kandian,free_kandian,user_createtime
FROM cps_user_{_num}.orders
where updatetime >= '{date}'
"""

sql_user_info = """
select id user_id,createtime,channel_id admin_id,sex,country,
    province,city,isp,referral_id,referral_id_permanent,ext
FROM cps_user_{_num}.user
where createtime >= '{date}'
"""

sql_dict_total_admin = """
SELECT a.id,a.nickname,a.createtime,c.nickname business_name,c.id business_id 
FROM cps.admin a
    left join cps.admin_extend b on a.id = b.admin_id
    left join cps.admin c on b.create_by = c.id;
"""

sql_dict_total_book = """
SELECT id,book_category_id,name,real_read_num,author,state,sex,price,is_finish,free_chapter_num,first_chapter_id,
    last_chapter_id,read_num,is_cp,cp_name,book_recharge 
FROM cps.book;
"""

sql_dict_update_referral = """
SELECT id,book_id,chapter_id,admin_id,chapter_name,cost,type,uv,follow,unfollow_num,net_follow_num,
    guide_chapter_idx,incr_num,money,orders_num,createtime,updatetime,state 
FROM cps.referral 
where updatetime >= '{date}';
"""

sql_referral_dict = """
SELECT id,book_id referral_book,chapter_id referral_chapter,admin_id referral_admin
FROM market_read.referral_info;
"""


"""
========================= -*- analysis_sql -*- =========================
以下是分析语句
"""

analysis_first_order = """
select date(from_unixtime(user_createtime)) logon_day,book_id,admin_id,
    date(from_unixtime(first_time)) order_day,count(*) order_user,sum(kandian/100) order_money,
    sum(if(type=2, 1, 0)) order_vip,sum(if(type=2, money, 0)) vip_money,1 order_type,{num} tab_num
from orders_log.orders_log_{num}
where state > 0 and first_time = createtime and date(from_unixtime(createtime)) >= '{date}'
group by logon_day,book_id,admin_id,order_day;
"""

analysis_repeat_order = """
select date(from_unixtime(user_createtime)) logon_day,book_id,admin_id,
    date(from_unixtime(createtime)) order_day,count(distinct user_id) order_user,
    count(user_id) order_times,sum(kandian/100) order_money,
    sum(if(type=2, 1, 0)) order_vip,sum(if(type=2, money, 0)) vip_money,2 order_type,{num} tab_num
from orders_log.orders_log_{num}
where state > 0 and first_time != createtime and date(from_unixtime(createtime)) >= '{date}'
group by logon_day,book_id,admin_id,order_day;
"""

analysis_logon_book_admin = """
select date(from_unixtime(createtime)) logon_day,referral_book book_id,admin_id,
    date(from_unixtime(createtime)) order_day, count(*) order_user,0 order_type,{num} tab_num
from user_info.user_info_{num}
where date(from_unixtime(createtime)) >= '{date}'
group by logon_day,book_id,admin_id,order_day
"""

analysis_compress_order_logon_conversion = """
select book_id,admin_id,order_day,logon_day,order_type,sum(order_user) order_user, sum(order_times) order_times, 
    sum(order_money) order_money, sum(order_vip) order_vip, sum(vip_money) vip_money
from {db}.{tab}
where order_day >= '{date}'
group by book_id,admin_id,order_day,logon_day,order_type
"""

analysis_reason_for_save = """
select count(*) nums,sum(bv) saves,sum(signv) reason_signs,sum(fdv) reason_fd,
    sum(kdv) reason_dk,sum(orderv) reason_order, 'all' types
from (SELECT distinct a.user_id, bv,signv,fdv,kdv,orderv FROM happy_seven.user_day_{tab_num} a
    left join (select distinct user_id,1 bv 
    from happy_seven.user_day_{tab_num} 
    where date_day = '{e_day}') b 
        on a.user_id = b.user_id
    left join (select distinct user_id,1 signv 
    from happy_seven.user_day_{tab_num}
    where date_day = '{e_day}' and signs > 0) sign 
        on a.user_id = sign.user_id
    left join (select distinct user_id,1 fdv 
    from happy_seven.user_day_{tab_num}
    where date_day = '{e_day}' and fd > 0) fd 
        on a.user_id = fd.user_id
    left join (select distinct user_id,1 kdv 
    from happy_seven.user_day_{tab_num}
    where date_day = '{e_day}' and kd > 0) kd 
        on a.user_id = kd.user_id
    left join (select distinct user_id,1 orderv 
    from happy_seven.user_day_{tab_num}
    where date_day = '{e_day}' and order_success > 0) orders 
        on a.user_id = orders.user_id
where date_day = '{s_day}') base
union all
select count(*) nums,sum(bv) saves,sum(signv) reason_signs,sum(fdv) reason_fd,
    sum(kdv) reason_dk,sum(orderv) reason_order, 'order' types
from (SELECT distinct a.user_id, bv,signv,fdv,kdv,orderv FROM happy_seven.user_day_{tab_num} a
    left join (select distinct user_id,1 bv 
    from happy_seven.user_day_{tab_num} 
    where date_day = '{e_day}') b 
        on a.user_id = b.user_id
    left join (select distinct user_id,1 signv 
    from happy_seven.user_day_{tab_num}
    where date_day = '{e_day}' and signs > 0) sign 
        on a.user_id = sign.user_id
    left join (select distinct user_id,1 fdv 
    from happy_seven.user_day_{tab_num}
    where date_day = '{e_day}' and fd > 0) fd 
        on a.user_id = fd.user_id
    left join (select distinct user_id,1 kdv 
    from happy_seven.user_day_{tab_num}
    where date_day = '{e_day}' and kd > 0) kd 
        on a.user_id = kd.user_id
    left join (select distinct user_id,1 orderv 
    from happy_seven.user_day_{tab_num}
    where date_day = '{e_day}' and order_success > 0) orders 
        on a.user_id = orders.user_id
where date_day = '{s_day}' and order_success > 0) base
union all
select count(*) nums,sum(bv) saves,sum(signv) reason_signs,sum(fdv) reason_fd,
    sum(kdv) reason_dk,sum(orderv) reason_order, 'logon' types
from (SELECT distinct a.user_id, bv,signv,fdv,kdv,orderv FROM happy_seven.user_day_{tab_num} a
    left join (select distinct user_id,1 bv 
    from happy_seven.user_day_{tab_num} 
    where date_day = '{e_day}') b 
        on a.user_id = b.user_id
    left join (select distinct user_id,1 signv 
    from happy_seven.user_day_{tab_num}
    where date_day = '{e_day}' and signs > 0) sign 
        on a.user_id = sign.user_id
    left join (select distinct user_id,1 fdv 
    from happy_seven.user_day_{tab_num}
    where date_day = '{e_day}' and fd > 0) fd 
        on a.user_id = fd.user_id
    left join (select distinct user_id,1 kdv 
    from happy_seven.user_day_{tab_num}
    where date_day = '{e_day}' and kd > 0) kd 
        on a.user_id = kd.user_id
    left join (select distinct user_id,1 orderv 
    from happy_seven.user_day_{tab_num}
    where date_day = '{e_day}' and order_success > 0) orders 
        on a.user_id = orders.user_id
where date_day = '{s_day}' and logon > 0) base
"""
