
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
SELECT user_id,date(from_unixtime(createtime)) date_day,
    sum(money) money,sum(money_benefit) money_benefit,
    sum(kandian) order_kd,sum(free_kandian) order_fd,
    sum(if(type='1', 1, 0)) bays,sum(if(type='2',1,0)) vips
FROM cps_user_{_num}.orders 
where createtime >= unix_timestamp('{date}') and state = '1' and deduct = 0
group by user_id,date_day;
"""

sql_retain_date_day = """
select * 
from {db}.{tab}
where date_day = '{date}'
"""

sql_retain_date_day_num = """
select * 
from happy_seven.user_day_{num}
where date_day = '{date}'
"""

sql_keep_user_admin_id = """
select user_id,admin_id
from user_info.user_info_{num}
"""

sql_keep_admin_id_name = """
SELECT id admin_id,nickname,business_name
from market_read.admin_info
"""

sql_retain_date_day_30 = """
select user_id,date_day
from {db}.{tab}
where date_day >= '{s_date}' and date_day < '{e_date}'
"""

sql_retain_date_day_30_num = """
select user_id,date_day
from happy_seven.user_day_{num}
where date_day >= '{s_date}' and date_day < '{e_date}'
"""

sql_keep_day_admin_count = """
SELECT date_day,admin_id,
    max(`year_month`) 'year_month', max(month_natural_week) month_natural_week,
    sum(logon_keep) logon_keep,sum(logon_2) logon_2,sum(logon_3) logon_3,sum(logon_7) logon_7,
    sum(logon_14) logon_14,sum(logon_30) logon_30,
    sum(order_keep) order_keep,sum(order_2) order_2,sum(order_3) order_3,sum(order_7) order_7,
    sum(order_14) order_14,sum(order_30) order_30,
    sum(all_keep) all_keep,sum(act_2) act_2,sum(act_3) act_3,sum(act_7) act_7,sum(act_14) act_14,sum(act_30) act_30
from {db}.{tab}
where date_day >= '{date}'
GROUP BY date_day,admin_id
"""

sql_read_last_date = """
SELECT MAX({dtype}) md FROM {db}.{tab}
"""

sql_delete_last_date = """
delete from {db}.{tab} where {type} >= '{date}'
"""

sql_delete_date_section = """
delete from {db}.{tab} where {type} >= '{date}' and {type} < '{date}'
"""

sql_delete_table_data = """
delete from {db}.{tab}
"""

sql_first_order_time = """
select user_id,min(createtime) first_time from cps_user_{_num}.orders
where state = '1' and deduct = 0 
group by user_id
"""

sql_order_info = """
select id,user_id,createtime,updatetime,state,type,book_id,chapter_id,admin_id,referral_id_permanent,
    money,money_benefit,benefit,kandian,free_kandian,user_createtime,deduct
FROM cps_user_{_num}.orders
where createtime >= '{date}'
"""

sql_user_info = """
select id user_id,createtime,updatetime,channel_id admin_id,sex,country,
    province,city,isp,referral_id,referral_id_permanent,ext
FROM cps_user_{_num}.user
where updatetime >= '{date}'
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

sql_user_info_kd_log = """
SELECT user_id,date(FROM_UNIXTIME(createtime)) logon_date,admin_id channel_id,referral_book
from user_info.user_info_{num}
"""

sql_order_log = """
SELECT book_id,admin_id,user_id,createtime,finishtime,type,state,deduct,kandian,free_kandian,money
from cps_user_{num}.orders
where createtime >= UNIX_TIMESTAMP('{s_date}') and createtime < UNIX_TIMESTAMP('{e_date}')
"""

sql_logon_log = """
SELECT channel_id admin_id,id user_id,createtime,state,0 type,0 kandian_balance,0 free_kandian
from cps_user_{num}.user
where createtime >= UNIX_TIMESTAMP('{s_date}') and createtime < UNIX_TIMESTAMP('{e_date}')
"""

sql_consume_log = """
SELECT user_id,5 type,type state,book_id,chapter_id,kandian,free_kandian,createtime
from cps_shard_{num}.consume
where createtime >= UNIX_TIMESTAMP('{s_date}') and createtime < UNIX_TIMESTAMP('{e_date}')
"""

sql_sign_log = """
SELECT uid user_id,3 type,kandian free_kandian,createtime
from cps_shard_{num}.sign
where createtime >= UNIX_TIMESTAMP('{s_date}') and createtime < UNIX_TIMESTAMP('{e_date}')
"""


"""
========================= -*- analysis_sql -*- =========================
以下是分析语句
"""

analysis_first_order = """
select date(from_unixtime(user_createtime)) logon_day,book_id,admin_id,
    date(from_unixtime(createtime)) order_day,count(distinct user_id) first_order_user,
    count(user_id) first_order_times,sum(money) first_order_money,{num} tab_num
from orders_log.orders_log_{num}
where state = 1 and deduct = 0 and first_time = createtime and referral_book = book_id
    and date(from_unixtime(createtime)) >= '{date}'
group by logon_day,book_id,admin_id,order_day;
"""

analysis_repeat_order = """
select date(from_unixtime(user_createtime)) logon_day,book_id,admin_id,
    date(from_unixtime(createtime)) order_day,count(distinct user_id) repeat_order_user,
    count(user_id) repeat_order_times,sum(money) repeat_order_money,{num} tab_num
from orders_log.orders_log_{num}
where state = 1 and deduct = 0 and first_time != createtime and referral_book = book_id 
    and date(from_unixtime(createtime)) >= '{date}'
group by logon_day,book_id,admin_id,order_day;
"""

analysis_first_repeat_order = """
SELECT logon_day,book_id,admin_id,order_day,count(DISTINCT user_id) first_repeat_order_user
from (select date(from_unixtime(user_createtime)) logon_day,book_id,admin_id,
            date(from_unixtime(min(createtime))) order_day,user_id user_id
        from orders_log.orders_log_{num}
        where state = 1 and deduct = 0 and first_time != createtime and referral_book = book_id 
            and date(from_unixtime(createtime)) >= '{date}'
        group by logon_day,book_id,admin_id,user_id) base 
group by logon_day,book_id,admin_id,order_day
"""

analysis_all_user_order = """
select date(from_unixtime(user_createtime)) logon_day,book_id,admin_id,
    date(from_unixtime(createtime)) order_day,count(distinct user_id) order_user,
    count(user_id) order_times,sum(money) order_money,{num} tab_num
from orders_log.orders_log_{num}
where state = 1 and deduct = 0 and date(from_unixtime(createtime)) >= '{date}'
group by logon_day,book_id,admin_id,order_day;
"""

analysis_vip_order = """
select date(from_unixtime(user_createtime)) logon_day,book_id,admin_id,
    date(from_unixtime(createtime)) order_day,count(distinct user_id) vip_order_user,
    count(user_id) vip_order_times,sum(money) vip_order_money,{num} tab_num
from orders_log.orders_log_{num}
where state = 1 and deduct = 0 and type=2 and referral_book = book_id 
    and date(from_unixtime(createtime)) >= '{date}'
group by logon_day,book_id,admin_id,order_day;
"""

analysis_logon_book_admin = """
select date(from_unixtime(createtime)) logon_day,referral_book book_id,admin_id,
    date(from_unixtime(createtime)) order_day, count(*) logon_user,{num} tab_num
from user_info.user_info_{num}
where date(from_unixtime(createtime)) >= '{date}'
group by logon_day,book_id,admin_id,order_day
"""

analysis_logon_book_id = """
select date(from_unixtime(createtime)) logon_day,referral_book book_id,admin_id,
    date(from_unixtime(createtime)) order_day, count(*) logon_user,{num} tab_num
from user_info.user_info_{num}
"""

analysis_compress_order_logon_conversion = """
select book_id,admin_id,order_day,logon_day,logon_user,
    order_user, order_times,order_money, 
    first_order_user,first_order_times, first_order_money, 
    repeat_order_user,repeat_order_times, repeat_order_money,first_repeat_order_user,
    vip_order_user,vip_order_times, vip_order_money
from {db}.{tab}
where order_day >= '{date}'
"""

analysis_keep_action_by_date_block = """
SELECT user_id,date(FROM_UNIXTIME(createtime)) action_date,book_id
from (SELECT user_id,createtime,type,book_id
        from log_block.action_log{date_code}_{num}
        where type=1 and createtime >= UNIX_TIMESTAMP('{s_date}') and deduct = 0 and state = 1
        union
        SELECT user_id,createtime,type,book_id
        from log_block.action_log{date_code}_{num}
        where type=2 and createtime >= UNIX_TIMESTAMP('{s_date}') and deduct = 0 and state = 1
        union
        SELECT user_id,createtime,type,book_id
        from log_block.action_log{date_code}_{num}
        where type=5 and createtime >= UNIX_TIMESTAMP('{s_date}')
) base
GROUP BY user_id,action_date,book_id
"""

analysis_keep_logon_by_date_block = """
SELECT user_id,date(FROM_UNIXTIME(createtime)) date_day,type,referral_book book_id
from log_block.action_log{date_code}_{num}
where type=0 and createtime >= UNIX_TIMESTAMP('{s_date}')
GROUP BY user_id,date_day,type,referral_book
"""

analysis_keep_order_by_date_block = """
SELECT user_id,date(FROM_UNIXTIME(createtime)) date_day,type,book_id
from (SELECT user_id,createtime,type,book_id
        from log_block.action_log{date_code}_{num}
        where type=1 and createtime >= UNIX_TIMESTAMP('{s_date}') and deduct = 0 and state = 1
        union
        SELECT user_id,createtime,type,book_id
        from log_block.action_log{date_code}_{num}
        where type=2 and createtime >= UNIX_TIMESTAMP('{s_date}') and deduct = 0 and state = 1
) base
GROUP BY user_id,date_day,type,book_id
"""

sql_retained_three_index_by_user_count = """
SELECT date_day,book_id,date_sub,action_date,type,sum(user_id) user_id
from {db}.{tab}
where date_day >= '{date}'
GROUP BY date_day,book_id,date_sub,action_date,type
"""

sql_retained_three_index_by_user_count_book_info = """
SELECT id book_id,name book_name
from market_read.book_info
"""

analysis_retained_logon_compress_thirty_day = """
SELECT logon_date,date(FROM_UNIXTIME(createtime)) date_day,referral_book book_id,
    channel_id,'激活用户数' type,count(DISTINCT user_id) times
from log_block.action_log{date_code}_{num}
where type = 0 and createtime >= UNIX_TIMESTAMP('{s_date}')
GROUP BY logon_date,date_day,referral_book,channel_id
union
SELECT logon_date,date(FROM_UNIXTIME(createtime)) date_day,book_id,
    channel_id,'订阅用户数' type,count(DISTINCT user_id) times
from log_block.action_log{date_code}_{num}
where type = 5 and createtime >= UNIX_TIMESTAMP('{s_date}') and referral_book = book_id
GROUP BY logon_date,date_day,book_id,channel_id
union
SELECT logon_date,date(FROM_UNIXTIME(createtime)) date_day,book_id,channel_id,
    '付费订阅用户' type,count(DISTINCT user_id) times
from log_block.action_log{date_code}_{num} 
where type = 5 and createtime >= UNIX_TIMESTAMP('{s_date}') and referral_book = book_id
    and kandian > 0
GROUP BY logon_date,date_day,book_id,channel_id
union
SELECT logon_date,date(FROM_UNIXTIME(createtime)) date_day,book_id,channel_id,'付费用户订阅章节' type,count(*) times
from log_block.action_log{date_code}_{num}
where type = 5 and createtime >= UNIX_TIMESTAMP('{s_date}') and referral_book = book_id
    and kandian > 0
GROUP BY logon_date,date_day,book_id,channel_id
union
SELECT logon_date,date(FROM_UNIXTIME(createtime)) date_day,book_id,
    channel_id,'免费订阅用户' type,count(DISTINCT user_id) times
from log_block.action_log{date_code}_{num}
where type = 5 and createtime >= UNIX_TIMESTAMP('{s_date}') and referral_book = book_id
    and free_kandian > 0
GROUP BY logon_date,date_day,book_id,channel_id
union
SELECT logon_date,date(FROM_UNIXTIME(createtime)) date_day,book_id,channel_id,
    '免费用户订阅章节' type,count(*) times
from log_block.action_log{date_code}_{num}
where type = 5 and createtime >= UNIX_TIMESTAMP('{s_date}') and referral_book = book_id
    and free_kandian > 0
GROUP BY logon_date,date_day,book_id,channel_id
union
SELECT logon_date,date(FROM_UNIXTIME(createtime)) date_day,book_id,channel_id,'vip订阅用户' type,
    count(DISTINCT user_id) times
from log_block.action_log{date_code}_{num}
where type = 5 and createtime >= UNIX_TIMESTAMP('{s_date}') and referral_book = book_id
    and free_kandian = 0 and kandian = 0
GROUP BY logon_date,date_day,book_id,channel_id
union
SELECT logon_date,date(FROM_UNIXTIME(createtime)) date_day,book_id,channel_id,'vip用户订阅章节' type,count(*) times
from log_block.action_log{date_code}_{num}
where type = 5 and createtime >= UNIX_TIMESTAMP('{s_date}') and referral_book = book_id
    and free_kandian = 0 and kandian = 0
GROUP BY logon_date,date_day,book_id,channel_id
"""

"""
************** -*- give up sql -*- **************
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
