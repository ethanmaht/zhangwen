
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
    sum(if(type='1', 1, 0)) bays,sum(if(type='2',1,0)) vips,1 order_success
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

sql_keep_book_id_name = """
SELECT id book_id,name book_name
from market_read.book_info
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

sql_retain_keep_admin_date_num = """
select *
from happy_seven.user_day_{num}
where date_day >= '{s_date}'
"""

sql_keep_book_admin_date_num = """
SELECT date(FROM_UNIXTIME(createtime)) date_day,'logon' type,user_id,referral_book book_id,channel_id,1 nums
from log_block.action_log{block}_{num}
where type=0 and createtime >= UNIX_TIMESTAMP('{s_date}')
GROUP BY date_day,user_id,referral_book,channel_id
union
SELECT date(FROM_UNIXTIME(createtime)) date_day,'order' type,user_id,book_id,channel_id,1 nums
from log_block.action_log{block}_{num}
where (type=1 or type=2) and createtime >= UNIX_TIMESTAMP('{s_date}')
GROUP BY date_day,user_id,book_id,channel_id
union
SELECT date(FROM_UNIXTIME(createtime)) date_day,'all' type,user_id,book_id,channel_id,1 nums
from log_block.action_log{block}_{num}
where createtime >= UNIX_TIMESTAMP('{s_date}')
GROUP BY date_day,user_id,book_id,channel_id
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

sql_keep_book_admin_count = """
SELECT date_day,book_id,channel_id,type,
    sum(nums) nums,sum(keep_2) keep_2,sum(keep_3) keep_3,sum(keep_7) keep_7,sum(keep_14) keep_14,sum(keep_30) keep_30
from {db}.{tab}
where date_day > '{date}'
GROUP BY date_day,book_id,channel_id,type
"""

sql_read_last_date = """
SELECT MAX({dtype}) md FROM {db}.{tab}
"""

sql_delete_last_date = """
delete from {db}.{tab} where {type} >= '{date}'
"""

sql_delete_last_date_num = """
delete from {db}.{tab} where {type} >= '{date}' and {num_type} = {num}
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
    money,money_benefit,benefit,kandian,free_kandian,user_createtime,deduct,activity_id
FROM cps_user_{_num}.orders
where createtime >= '{date}'
"""

sql_user_info = """
select id user_id,createtime,updatetime,channel_id admin_id,sex,country,is_subscribe,
    province,city,isp,referral_id,referral_id_permanent
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
    last_chapter_id,read_num,is_cp,cp_name,book_recharge,createtime,updatetime,chapter_num
FROM cps.book
where id < 10000000
union
SELECT b.id,book_category_id,name,real_read_num,author,state,sex,price,is_finish,free_chapter_num,first_chapter_id,
    last_chapter_id,read_num,is_cp,cp_name,book_recharge,createtime,updatetime,b_r.chapter_num
from cps.book b
left join (
    SELECT id + 10000000 id,
    if(
    (LENGTH(last_chapter_id)=11 or LENGTH(last_chapter_id)=14), 
    last_chapter_id div 10,
    last_chapter_id) % 10000 chapter_num
    from cps.book where id < 10000000
) b_r on b.id = b_r.id
where b.id > 10000000
"""

sql_book_channel_price = """
SELECT *
from cps.book_channel_price
"""

sql_podcast_episodes = """
SELECT id,podcast_id book_id,origin_id,sequence chapter_id,duration,created_at,updated_at,deleted_at
from cps.podcast_episodes
"""

sql_podcasts = """
SELECT *
from cps.podcasts
"""

sql_dict_update_referral = """
SELECT id,book_id,chapter_id,admin_id,chapter_name,cost,type,uv,follow,unfollow_num,net_follow_num,
    guide_chapter_idx,incr_num,money,orders_num,createtime,updatetime,state 
FROM cps.referral 
where updatetime >= '{date}';
"""


sql_dict_update_custom = """
SELECT id custom_id,admin_id,sendtime,user_json,createtime,updatetime,statue,send_num,message_type
FROM cps.custom
where sendtime >= '{date}';
"""

sql_dict_update_custom_url = """
SELECT id,title,custom_id,book_id,book_name
FROM cps.custom_url
where sendtime >= '{date}';
"""

sql_dict_update_custom_url_collect = """
SELECT * from cps.custom_url_collect
"""

sql_dict_update_custom_url_activity = """
SELECT * from cps.activity
"""

sql_dict_update_templatemessage = """
SELECT * from cps.templatemessage
"""

sql_dict_update_mp_send = """
SELECT * from cps.mp_send
"""

sql_dict_update_referral_sound = """
SELECT id,book_id,chapter_id,admin_id,chapter_name,cost,type,uv,follow,unfollow_num,net_follow_num,
    guide_chapter_idx,incr_num,money,orders_num,createtime,updatetime,state 
FROM cps.referral 
"""

sql_referral_dict = """
SELECT id,book_id referral_book,chapter_id referral_chapter,admin_id referral_admin
FROM market_read.referral_info;
"""

sql_user_info_kd_log = """
SELECT user_id,date(FROM_UNIXTIME(createtime)) logon_date,admin_id channel_id,referral_book
from user_info.user_info_{num}
"""

sql_book_channel_price_local = """
SELECT admin_id channel_id,book_id,channel_free_chapter_num,channel_price
from market_read.book_channel_price
"""

sql_book_price_local = """
SELECT id book_id,price,free_chapter_num,
    if((LENGTH(last_chapter_id)=11 or LENGTH(last_chapter_id)=14), 
        last_chapter_id div 10,
            last_chapter_id) % 10000 last_chapter_id,
    createtime book_create,updatetime book_update,is_finish
from market_read.book_info
"""

sql_recently_read_data = """
SELECT id,user_id,book_id,
    if((LENGTH(chapter_id)=11 or LENGTH(chapter_id)=14), chapter_id div 10,chapter_id) % 10000 chapter_id,
    createtime,updatetime,user_type
from cps_shard_{num}.user_recently_read
where updatetime >= UNIX_TIMESTAMP('{s_date}')
"""

sql_lase_date_user_recently_read_data = """
SELECT date(FROM_UNIXTIME(max(updatetime))) md
from user_read.user_read_{num}
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

sql_user_sign_count = """
SELECT uid user_id,sum(kandian) sign_kd,sum(box) sign_box,max(day) continuity
from cps_shard_{num}.sign
where createtime >= UNIX_TIMESTAMP('{s_date}') and createtime < UNIX_TIMESTAMP('{e_date}')
GROUP BY uid
"""

sql_order_count = """
SELECT user_id,sum(if(state='0',1,0)) fail_order,sum(if(state='1',1,0)) pay_order,sum(if(state='1',money,0)) order_money
from cps_user_{num}.orders
where deduct = '0' and createtime >= UNIX_TIMESTAMP('{s_date}') and createtime < UNIX_TIMESTAMP('{e_date}')
GROUP BY user_id
"""

sql_user_id_channel = """
SELECT id user_id,channel_id
from cps_user_{num}.user
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
from (
    select date(from_unixtime(user_createtime)) logon_day,book_id,admin_id,
        date(from_unixtime(min(createtime))) order_day,user_id user_id
    from orders_log.orders_log_{num}
    where state = 1 and deduct = 0 and first_time != createtime and referral_book = book_id 
        and date(from_unixtime(createtime)) >= '{date}'
    group by logon_day,book_id,admin_id,user_id
) base 
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
from (
    SELECT user_id,createtime,type,book_id
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
from (
    SELECT user_id,createtime,type,book_id
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

sql_retained_logon_compress_thirty_day_count = """
SELECT logon_date,date_sub,book_id,channel_id,type,sum(times) times
from  {db}.{tab}
where logon_date >= '{date}'
GROUP BY logon_date,date_sub,book_id,channel_id,type
"""

sql_retained_three_index_by_user_count_book_info = """
SELECT id book_id,name book_name
from market_read.book_info
"""

sql_retained_admin_info = """
SELECT id channel_id,nickname,business_name
from market_read.admin_info
"""

sql_book_admin_read = """
SELECT book_id,channel_id,last_chapter_id,is_finish,start_date,count(*) start_book,
sum(over_free) over_free, sum(over_100) over_100,sum(over_200) over_200, sum(over_300) over_300,
sum(over_500) over_500,sum(over_750) over_750,sum(over_1000) over_1000,sum(over_2000) over_2000,
sum(over_book) over_book
from(
    SELECT book_id,channel_id,last_chapter_id,is_finish,logon_date,
        date(FROM_UNIXTIME(book_create)) book_create,date(FROM_UNIXTIME(createtime)) start_date,
        if(chapter_id>free_chapter,1,0) over_free,if(chapter_id>=100,1,0) over_100,
        if(chapter_id>=200,1,0) over_200,if(chapter_id>=300,1,0) over_300,if(chapter_id>=500,1,0) over_500,
        if(chapter_id>=750,1,0) over_750,if(chapter_id>=1000,1,0) over_1000,if(chapter_id>=2000,1,0) over_2000,
        if(chapter_id=last_chapter_id,1,0) over_book
    from (
        SELECT a.book_id,a.channel_id,b.is_finish,a.createtime,a.updatetime,book_create,logon_date,
            CAST(if(a.channel_free_chapter_num<>0,a.channel_free_chapter_num,
            if(a.free_chapter_num<>0,a.free_chapter_num,15)) AS SIGNED) free_chapter,
            CAST(b.chapter_num AS SIGNED) last_chapter_id,CAST(chapter_id AS SIGNED) chapter_id
        from user_read.user_read_{num} a
        left join market_read.book_info b on b.id = a.book_id
        where a.createtime >= UNIX_TIMESTAMP('{date}')
    ) base
) box
GROUP BY book_id,channel_id,last_chapter_id,is_finish,start_date
"""

sql_book_admin_read_step_30 = """
SELECT book_id,channel_id,last_chapter_id,is_finish,start_date,count(*) start_book,
    sum(over_free) over_free, sum(over_30) over_30,sum(over_60) over_60, sum(over_90) over_90,
    sum(over_120) over_120,sum(over_150) over_150,sum(over_180) over_180,sum(over_210) over_210,
    sum(over_240) over_240,sum(over_270) over_270,sum(over_300) over_300,sum(over_400) over_400,
    sum(over_500) over_500,sum(over_600) over_600,sum(over_700) over_700,sum(over_800) over_800,
    sum(over_1000) over_1000,sum(over_book) over_book
from(
    SELECT book_id,channel_id,last_chapter_id,is_finish,logon_date,
        date(FROM_UNIXTIME(book_create)) book_create,date(FROM_UNIXTIME(createtime)) start_date,
        if(chapter_id>=free_chapter,1,0) over_free,if(chapter_id>=30,1,0) over_30,
        if(chapter_id>=60,1,0) over_60,if(chapter_id>=90,1,0) over_90,if(chapter_id>=120,1,0) over_120,
        if(chapter_id>=150,1,0) over_150,if(chapter_id>=180,1,0) over_180,if(chapter_id>=210,1,0) over_210,
        if(chapter_id>=240,1,0) over_240,if(chapter_id>=270,1,0) over_270,if(chapter_id>=300,1,0) over_300,
        if(chapter_id>=400,1,0) over_400,if(chapter_id>=500,1,0) over_500,if(chapter_id>=600,1,0) over_600,
        if(chapter_id>=700,1,0) over_700,if(chapter_id>=800,1,0) over_800,if(chapter_id>=1000,1,0) over_1000,
        if(chapter_id>=last_chapter_id,1,0) over_book
    from (
        SELECT a.book_id,a.channel_id,b.is_finish,a.createtime,a.updatetime,book_create,logon_date,
            CAST(if(a.channel_free_chapter_num<>0,a.channel_free_chapter_num,
            if(a.free_chapter_num<>0,a.free_chapter_num,15)) AS SIGNED) free_chapter,
            CAST(b.chapter_num AS SIGNED) last_chapter_id,CAST(chapter_id AS SIGNED) chapter_id
        from user_read.user_read_{num} a
        left join market_read.book_info b on b.id = a.book_id
        where a.createtime >= UNIX_TIMESTAMP('{date}')
    ) base
) box
GROUP BY book_id,channel_id,last_chapter_id,is_finish,start_date
"""

sql_book_admin_read_step_new_user_30 = """
SELECT book_id,channel_id,last_chapter_id,is_finish,start_date,count(*) start_book,
    sum(over_free) over_free, sum(over_30) over_30,sum(over_60) over_60, sum(over_90) over_90,
    sum(over_120) over_120,sum(over_150) over_150,sum(over_180) over_180,sum(over_210) over_210,
    sum(over_240) over_240,sum(over_270) over_270,sum(over_300) over_300,sum(over_400) over_400,
    sum(over_500) over_500,sum(over_600) over_600,sum(over_700) over_700,sum(over_800) over_800,
    sum(over_1000) over_1000,sum(over_book) over_book
from(
    SELECT book_id,channel_id,last_chapter_id,is_finish,logon_date,
        date(FROM_UNIXTIME(book_create)) book_create,date(FROM_UNIXTIME(createtime)) start_date,
        if(chapter_id>=free_chapter,1,0) over_free,if(chapter_id>=30,1,0) over_30,
        if(chapter_id>=60,1,0) over_60,if(chapter_id>=90,1,0) over_90,if(chapter_id>=120,1,0) over_120,
        if(chapter_id>=150,1,0) over_150,if(chapter_id>=180,1,0) over_180,if(chapter_id>=210,1,0) over_210,
        if(chapter_id>=240,1,0) over_240,if(chapter_id>=270,1,0) over_270,if(chapter_id>=300,1,0) over_300,
        if(chapter_id>=400,1,0) over_400,if(chapter_id>=500,1,0) over_500,if(chapter_id>=600,1,0) over_600,
        if(chapter_id>=700,1,0) over_700,if(chapter_id>=800,1,0) over_800,if(chapter_id>=1000,1,0) over_1000,
        if(chapter_id>=last_chapter_id,1,0) over_book
    from (
        SELECT a.book_id,a.channel_id,b.is_finish,a.createtime,a.updatetime,book_create,logon_date,
            CAST(if(a.channel_free_chapter_num<>0,a.channel_free_chapter_num,
            if(a.free_chapter_num<>0,a.free_chapter_num,15)) AS SIGNED) free_chapter,
            CAST(b.chapter_num AS SIGNED) last_chapter_id,CAST(chapter_id AS SIGNED) chapter_id
        from user_read.user_read_{num} a
        left join market_read.book_info b on b.id = a.book_id 
        where a.logon_date >= '{date}' and a.book_id = a.referral_book
    ) base
) box
GROUP BY book_id,channel_id,last_chapter_id,is_finish,start_date
"""

sql_book_admin_read_count = """
SELECT book_id,channel_id,last_chapter_id,is_finish,start_date,sum(start_book) start_book,
    sum(over_free) over_free,sum(over_free) / sum(start_book) over_free_p, 
    sum(over_100) over_100,sum(over_100) / sum(start_book) over_100_p, 
    sum(over_200) over_200, sum(over_200) / sum(start_book) over_200_p, 
    sum(over_300) over_300, sum(over_300) / sum(start_book) over_300_p, 
    sum(over_500) over_500, sum(over_500) / sum(start_book) over_500_p, 
    sum(over_750) over_750, sum(over_750) / sum(start_book) over_750_p,
    sum(over_1000) over_1000,sum(over_1000) / sum(start_book) over_1000_p, 
    sum(over_2000) over_2000,sum(over_2000) / sum(start_book) over_2000_p, 
    sum(over_book) over_book,sum(over_book) / sum(start_book) over_book_p
from {db}.{tab}
GROUP BY book_id,channel_id,last_chapter_id,is_finish,start_date
"""

sql_book_admin_read_count_30 = """
SELECT book_id,channel_id,last_chapter_id,is_finish,start_date,sum(start_book) start_book,
    sum(over_free) over_free,sum(over_30) over_30,sum(over_60) over_60, 
    sum(over_90) over_90, sum(over_120) over_120, sum(over_150) over_150, 
    sum(over_180) over_180, sum(over_210) over_210, sum(over_240) over_240, 
    sum(over_270) over_270, sum(over_300) over_300, sum(over_400) over_400, 
    sum(over_500) over_500, sum(over_600) over_600, sum(over_700) over_700, 
    sum(over_800) over_800,sum(over_1000) over_1000,sum(over_book) over_book
from {db}.{tab}
GROUP BY book_id,channel_id,last_chapter_id,is_finish,start_date
"""

sql_book_admin_read_count_30_day_ladder = """
SELECT book_id,channel_id,last_chapter_id,is_finish,start_date,sum(start_book) start_book,
    sum(over_free) over_free,sum(over_30) over_30,sum(over_60) over_60, 
    sum(over_90) over_90, sum(over_120) over_120, sum(over_150) over_150, 
    sum(over_180) over_180, sum(over_210) over_210, sum(over_240) over_240, 
    sum(over_270) over_270, sum(over_300) over_300, sum(over_400) over_400, 
    sum(over_500) over_500, sum(over_600) over_600, sum(over_700) over_700, 
    sum(over_800) over_800,sum(over_1000) over_1000,sum(over_book) over_book
from {db}.{tab}
where start_date >= '{s_date}' and start_date < '{e_date}' 
GROUP BY book_id,channel_id,last_chapter_id,is_finish,start_date
"""

analysis_retained_logon_compress_thirty_day = """
SELECT date(logon_date) logon_date,date(FROM_UNIXTIME(createtime)) date_day,referral_book book_id,
    channel_id,'激活用户数' type,count(DISTINCT user_id) times
from log_block.action_log{date_code}_{num}
where type = 0 and logon_date >= '{s_date}'
GROUP BY logon_date,date_day,referral_book,channel_id
union
SELECT date(logon_date) logon_date,date(FROM_UNIXTIME(createtime)) date_day,book_id,
    channel_id,'订阅用户数' type,count(DISTINCT user_id) times
from log_block.action_log{date_code}_{num}
where type = 5 and logon_date >= '{s_date}' and referral_book = book_id
GROUP BY logon_date,date_day,book_id,channel_id
union
SELECT date(logon_date) logon_date,date(FROM_UNIXTIME(createtime)) date_day,book_id,channel_id,
    '付费订阅用户' type,count(DISTINCT user_id) times
from log_block.action_log{date_code}_{num} 
where type = 5 and logon_date >= '{s_date}' and referral_book = book_id
    and kandian > 0
GROUP BY logon_date,date_day,book_id,channel_id
union
SELECT date(logon_date) logon_date,date(FROM_UNIXTIME(createtime)) date_day,
    book_id,channel_id,'付费用户订阅章节' type,count(*) times
from log_block.action_log{date_code}_{num}
where type = 5 and logon_date >= '{s_date}' and referral_book = book_id
    and kandian > 0
GROUP BY logon_date,date_day,book_id,channel_id
union
SELECT date(logon_date) logon_date,date(FROM_UNIXTIME(createtime)) date_day,book_id,
    channel_id,'免费订阅用户' type,count(DISTINCT user_id) times
from log_block.action_log{date_code}_{num}
where type = 5 and logon_date >= '{s_date}' and referral_book = book_id
    and free_kandian > 0
GROUP BY logon_date,date_day,book_id,channel_id
union
SELECT date(logon_date) logon_date,date(FROM_UNIXTIME(createtime)) date_day,book_id,channel_id,
    '免费用户订阅章节' type,count(*) times
from log_block.action_log{date_code}_{num}
where type = 5 and logon_date >= '{s_date}' and referral_book = book_id
    and free_kandian > 0
GROUP BY logon_date,date_day,book_id,channel_id
union
SELECT date(logon_date) logon_date,date(FROM_UNIXTIME(createtime)) date_day,book_id,channel_id,'vip订阅用户' type,
    count(DISTINCT user_id) times
from log_block.action_log{date_code}_{num}
where type = 5 and logon_date >= '{s_date}' and referral_book = book_id
    and free_kandian = 0 and kandian = 0
GROUP BY logon_date,date_day,book_id,channel_id
union
SELECT date(logon_date) logon_date,date(FROM_UNIXTIME(createtime)) date_day,
    book_id,channel_id,'vip用户订阅章节' type,count(*) times
from log_block.action_log{date_code}_{num}
where type = 5 and logon_date >= '{s_date}' and referral_book = book_id
    and free_kandian = 0 and kandian = 0
GROUP BY logon_date,date_day,book_id,channel_id
"""

sound_user_log = """
SELECT date(FROM_UNIXTIME(u.createtime)) logon_day,user_id,1 logon_user,is_subscribe,
    referral_id_permanent,u.admin_id,a.business_name,a.nickname
from sound.`user` u
left join sound.admin a on a.id = u.admin_id
where u.createtime >= UNIX_TIMESTAMP('{s_date}')
"""

sount_order_log = """
SELECT user_id,count(*) order_times,sum(money) money,1 order_users
from sound.orders
where state = 1 and deduct = 0 and createtime >= UNIX_TIMESTAMP('{s_date}')
GROUP BY user_id
"""

sound_referral_info = """
SELECT r.id referral_id_permanent,b.`name` logon_book,c.channel_free_chapter_num
from sound.referral r
left join sound.book b on b.id = r.book_id
left join sound.channel_price c on c.admin_id = r.admin_id and c.book_id = r.book_id
"""

sound_keep = """
SELECT logon_day,user_id,1 keep
from (
    SELECT date(date_add(time, interval -1 day)) logon_day,uid user_id
    from sound.es_log l
    where time >= '{s_date}'
    union
    SELECT date(date_add(FROM_UNIXTIME(createtime), interval -1 day)) logon_day,user_id
    from sound.consume
    where createtime >= UNIX_TIMESTAMP('{s_date}')
    union
    SELECT date(date_add(FROM_UNIXTIME(createtime), interval -1 day)) logon_day,user_id
    from sound.sign
    where createtime >= UNIX_TIMESTAMP('{s_date}')
) base
GROUP BY logon_day,user_id
"""

sound_chapter_pv_uv = """
SELECT base.*,o.order_pv,o.order_uv,pp.pay_page_uv,c.channel_free_chapter_num,
    if(c.channel_free_chapter_num*1>=base.chapter_num*1,'免费','收费') 受否免费,
    a.business_name,a.nickname,b.name book_name
from (
    SELECT date(l.time) date_day,l.book_id,l.chapter_id,pe.chapter_id chapter_num,
        l.admin_id,count(*) pv,count(DISTINCT l.uid) uv
    from (
    SELECT l.time,if(s.user_sid = 1,l.sid,l.chapter_id) chapter_id,l.book_id,l.admin_id,l.uid
    from sound.es_log l
    left join (
        SELECT book_id,uid,min(time) time,1 user_sid
        from sound.es_log
        where sid > 0
        GROUP BY book_id,uid
    ) s on s.book_id = l.book_id and s.uid = l.uid and s.time = l.time
    where page = '/index/book/chapter'
    ) l
    left join sound.podcasts p on l.book_id = p.id
    left join sound.podcast_episodes pe on p.origin_id = pe.book_id and l.chapter_id = pe.id
    where l.book_id > 0 and l.chapter_id > 0 and l.time > '{s_date}'
    GROUP BY l.book_id,l.chapter_id,pe.chapter_id,l.admin_id,date_day
) base
left join (
    SELECT date(time) date_day,l.book_id,l.chapter_id,admin_id,count(DISTINCT uid) pay_page_uv
    from sound.es_log l
    where l.book_id > 0 and l.chapter_id > 0 and page like '%/index/recharge/pay%'
    GROUP BY date_day,l.book_id,l.chapter_id,admin_id
) pp on base.date_day = pp.date_day and base.book_id = pp.book_id 
    and base.chapter_id = pp.chapter_id and base.admin_id = pp.admin_id
left join (
    SELECT date(FROM_UNIXTIME(createtime)) date_day,chapter_id,admin_id,book_id,
        count(*) order_pv,count(DISTINCT user_id) order_uv
    from sound.`orders`
    GROUP BY chapter_id,admin_id,book_id,date_day
) o on base.date_day = o.date_day and base.book_id = o.book_id 
    and base.chapter_id = o.chapter_id and base.admin_id = o.admin_id
left join (
    SELECT c.book_id,c.admin_id,c.channel_free_chapter_num
    from sound.channel_price c
) c on c.book_id = base.book_id and base.admin_id = c.admin_id
left join sound.admin a on a.id = base.admin_id
left join sound.book b on b.id = base.book_id
"""

sql_user_order_portrait = """
SELECT isp,province,city,sex,y_m,monet_box,user_times order_times,type,sum(money) order_money,
    sum(order_times) order_count,count(DISTINCT user_id) ordre_users,o_y_m,first_sub
from (
    SELECT o.money monet_box,DATE_FORMAT(FROM_UNIXTIME(u.createtime),'%Y-%m-01') y_m,o.user_id,
        TIMESTAMPDIFF(DAY,date(FROM_UNIXTIME(u.createtime)),first_date) first_sub,
        count(*) order_times,user_times,sum(money) money,u.sex,u.province,u.city,u.isp,type,
        DATE_FORMAT(FROM_UNIXTIME(o.createtime),'%Y-%m-01') o_y_m
    from cps_user_{num}.orders o
    left join cps_user_{num}.`user` u on u.id = o.user_id
    left join (
        SELECT user_id,count(*) user_times,min(date(FROM_UNIXTIME(o.createtime))) first_date
        from cps_user_{num}.orders o
        where deduct = '0' and state = '1'
        GROUP BY user_id
    ) f on f.user_id = o.user_id
    where o.deduct = '0' and o.state = '1' 
        and o.createtime >= UNIX_TIMESTAMP('{s_date}') and o.createtime < UNIX_TIMESTAMP('{e_date}')
    GROUP BY o.user_id,money,type,o_y_m
) base
GROUP BY isp,province,city,sex,y_m,monet_box,user_times,o_y_m,first_sub
"""

sql_user_order_portrait_admin_book = """
SELECT isp,province,city,sex,y_m,monet_box,user_times order_times,type,sum(money) order_money,
    sum(order_times) order_count,count(DISTINCT user_id) ordre_users,o_y_m,first_sub,admin_id,book_id
from (
    SELECT o.money monet_box,DATE_FORMAT(FROM_UNIXTIME(u.createtime),'%Y-%m-01') y_m,o.user_id,
        TIMESTAMPDIFF(DAY,date(FROM_UNIXTIME(u.createtime)),first_date) first_sub,
        count(*) order_times,user_times,sum(money) money,u.sex,u.province,u.city,u.isp,type,
        DATE_FORMAT(FROM_UNIXTIME(o.createtime),'%Y-%m-01') o_y_m,admin_id,book_id
    from cps_user_{num}.orders o
    left join cps_user_{num}.`user` u on u.id = o.user_id
    left join (
        SELECT user_id,count(*) user_times,min(date(FROM_UNIXTIME(o.createtime))) first_date
        from cps_user_{num}.orders o
        where deduct = '0' and state = '1'
        GROUP BY user_id
    ) f on f.user_id = o.user_id
    where o.deduct = '0' and o.state = '1' 
        and o.createtime >= UNIX_TIMESTAMP('{s_date}') and o.createtime < UNIX_TIMESTAMP('{e_date}')
    GROUP BY o.user_id,money,type,o_y_m,admin_id,book_id
) base
GROUP BY isp,province,city,sex,y_m,monet_box,user_times,o_y_m,first_sub
"""

sql_user_order_portrait_admin_book_count = """
SELECT isp,province,city,p.sex,y_m,monet_box,order_times,o_y_m,admin_id,book_id,type,sum(order_money) order_money,
    sum(order_count) order_count,sum(ordre_users) ordre_users,a.nickname,a.business_name,b.name book_name,first_sub
from market_read.portrait_user_order_admin_book p
left join market_read.book_info b on b.id = book_id
left join market_read.admin_info a on a.id = admin_id
where y_m >= '{s_date}' and y_m < '{_date}'
GROUP BY isp,province,city,p.sex,y_m,monet_box,order_times,
    o_y_m,admin_id,book_id,type,a.nickname,a.business_name,b.name,first_sub
"""


""" ****** -*- conversion funnel -*- ******"""

sql_first_order = """
SELECT user_id,date(FROM_UNIXTIME(min(first_time))) first_time,1 first_order
from orders_log.orders_log_{num}
where state=1 and deduct=0 and referral_book = book_id and first_time = createtime 
GROUP BY user_id
"""

sql_first_order_not_same_book = """
SELECT user_id,date(FROM_UNIXTIME(min(first_time))) first_time,1 first_order
from orders_log.orders_log_{num}
where state=1 and deduct=0 and first_time = createtime 
GROUP BY user_id
"""

sql_first_recharge_order = """
SELECT user_id,date(FROM_UNIXTIME(min(createtime))) recharge_time,1 recharge_order
from orders_log.orders_log_{num}
where state=1 and deduct=0 and referral_book = book_id and first_time != createtime 
GROUP BY user_id
"""

sql_first_recharge_order_not_same_book = """
SELECT user_id,date(FROM_UNIXTIME(min(createtime))) recharge_time,1 recharge_order
from orders_log.orders_log_{num}
where state=1 and deduct=0 and first_time != createtime 
GROUP BY user_id
"""

sql_logon_user = """
SELECT user_id,date(FROM_UNIXTIME(createtime)) logon_date,admin_id,referral_book book_id,is_subscribe,1 logon_user
from user_info.user_info_{num}
where createtime >= UNIX_TIMESTAMP('{s_date}')
"""

sql_user_read = """
SELECT user_id,book_id,
    max(if(CONVERT(chapter_id,SIGNED)>=if(free_chapter_num=0,15,CONVERT(free_chapter_num,SIGNED)),1,0)) pass_free
from user_read.user_read_{num}
where logon_date >= '{s_date}' and referral_book = book_id
group by user_id,book_id
"""

sql_user_read_not_same_book = """
SELECT user_id,book_id,
    max(if(CONVERT(chapter_id,SIGNED)>=if(free_chapter_num=0,15,CONVERT(free_chapter_num,SIGNED)),1,0)) pass_free
from user_read.user_read_{num}
where logon_date >= '{s_date}'
group by user_id,book_id
"""

sql_consume = """
SELECT user_id,1 consume
from log_block.action_log{block}_{num}
where type = 5 and book_id = referral_book and createtime >= UNIX_TIMESTAMP('{s_date}')
GROUP BY user_id,book_id
"""

sql_consume_not_same_book = """
SELECT user_id,1 consume
from log_block.action_log{block}_{num}
where type = 5 and createtime >= UNIX_TIMESTAMP('{s_date}')
GROUP BY user_id,book_id
"""

sql_conversion_funnel_count = """
SELECT logon_date,book_id,admin_id channel_id,
    sum(logon_user) logon_user,sum(pass_free) pass_free,sum(is_subscribe) is_subscribe,
    sum(if(first_sub>=0 and first_sub<1,first_order,0)) 'first_order_day',
    sum(if(first_sub>=0 and first_sub<3,first_order,0)) 'first_order_3day',
    sum(if(recharge_sub>=0 and recharge_sub<1,recharge_order,0)) 'recharge_order_day',
    sum(if(recharge_sub>=0 and recharge_sub<3,recharge_order,0)) 'recharge_order_3day',
    sum(if(recharge_sub>=0 and recharge_sub<7,recharge_order,0)) 'recharge_order_7day',
    sum(if(recharge_sub>=0 and recharge_sub<14,recharge_order,0)) 'recharge_order_14day',
    sum(if(recharge_sub>=0 and recharge_sub<30,recharge_order,0)) 'recharge_order_30day',
    sum(if(recharge_sub>=0 and recharge_sub<60,recharge_order,0)) 'recharge_order_60day',
    sum(if(recharge_sub>=0 and recharge_sub<90,recharge_order,0)) 'recharge_order_90day'
from {db}.{tab}
GROUP BY logon_date,book_id,admin_id
"""

""" interval """
sql_user_date_interval = """
SELECT user_id,date(FROM_UNIXTIME(createtime)) date_day
from log_block.action_log{_block}_{num}
GROUP BY user_id,date_day
"""


""" """

sql_logon_users = """
SELECT date(FROM_UNIXTIME(createtime)) date_day,count(*) logon_user
from user_info.user_info_{num}
where createtime >= UNIX_TIMESTAMP('{s_date}')
GROUP BY date_day
"""

sql_logon_users_ym = """
SELECT DATE_FORMAT(FROM_UNIXTIME(createtime),'%Y-%m-01') date_month,count(*) logon_user
from user_info.user_info_{num}
where createtime >= UNIX_TIMESTAMP('{s_date}')
GROUP BY date_month
"""

sql_order_users_money = """
SELECT date(FROM_UNIXTIME(createtime)) date_day,
    count(DISTINCT user_id) order_user,sum(money) money,
    sum(if(first_time=createtime,money,0)) first_money,
        count(DISTINCT if(first_time=createtime,user_id,Null)) first_user,
    sum(if(first_time!=createtime,money,0)) re_money,
        count(DISTINCT if(first_time!=createtime,user_id,Null)) re_user,
    sum(if(type=2,money,0)) vip_money,count(DISTINCT if(type=2,user_id,Null)) vip_user
from orders_log.orders_log_{num}
where createtime >= UNIX_TIMESTAMP('{s_date}') and state = 1 and deduct = 0
GROUP BY date_day
"""

sql_order_users_money_ym = """
SELECT DATE_FORMAT(FROM_UNIXTIME(createtime),'%Y-%m-01') date_month,
    count(DISTINCT user_id) order_user,sum(money) money,
    sum(if(first_time=createtime,money,0)) first_money,
        count(DISTINCT if(first_time=createtime,user_id,Null)) first_user,
    sum(if(first_time!=createtime,money,0)) re_money,
        count(DISTINCT if(first_time!=createtime,user_id,Null)) re_user,
    sum(if(type=2,money,0)) vip_money,count(DISTINCT if(type=2,user_id,Null)) vip_user
from orders_log.orders_log_{num}
where createtime >= UNIX_TIMESTAMP('{s_date}') and state = 1 and deduct = 0
GROUP BY date_month
"""

sql_active = """
SELECT date(date_day) date_day,count(DISTINCT user_id) active_user
from user_interval.user_date_interval_{num}
where date_day >= '{s_date}'
GROUP BY date_day
"""

sql_active_ym = """
SELECT DATE_FORMAT(date_day,'%Y-%m-01') date_month,count(DISTINCT user_id) active_user
from user_interval.user_date_interval_{num}
where date_day >= '{s_date}'
GROUP BY date_month
"""

sql_back = """
SELECT date(next_date) date_day,count(DISTINCT if(day_sub>6,user_id,Null)) back_user
from user_interval.user_date_interval_{num}
where next_date >= '{s_date}'
GROUP BY next_date
"""

sql_back_ym = """
SELECT DATE_FORMAT(next_date,'%Y-%m-01') date_month,count(DISTINCT if(day_sub>6,user_id,Null)) back_user
from user_interval.user_date_interval_{num}
where next_date >= '{s_date}'
GROUP BY date_month
"""

sql_user_date_consume = """
SELECT date(FROM_UNIXTIME(createtime)) date_day,count(DISTINCT book_id,user_id) comsum_book,
    count(*) comsum,count(DISTINCT user_id) comsum_user
from log_block.action_log{_block}_{num}
where createtime >= UNIX_TIMESTAMP('{s_date}') and type = 5
GROUP BY date_day
"""

sql_user_date_consume_ym = """
SELECT DATE_FORMAT(FROM_UNIXTIME(createtime),'%Y-%m-01') date_month,count(DISTINCT book_id,user_id) comsum_book,
    count(*) comsum,count(DISTINCT user_id) comsum_user
from log_block.action_log{_block}_{num}
where createtime >= UNIX_TIMESTAMP('{s_date}') and type = 5
GROUP BY date_month
"""

sql_book_date_consume = """
SELECT date(FROM_UNIXTIME(createtime)) date_day,book_id,
    count(*) comsume,count(DISTINCT user_id) comsume_user
from log_block.action_log{_block}_{num}
where createtime >= UNIX_TIMESTAMP('{s_date}') and type = 5
GROUP BY date_day,book_id
"""


""" ************** -*- analysis -*- ************** """

sql_order_save = """
SELECT 
    date_day,
    first_day,
    first_sub,
    user_id,
    user_sub,
    if(is_first=1,'首充',if(first_sub=0,'当日',if(first_sub=1,'次日',
    if(first_sub=2,'3日',if(first_sub=3,'4日',if(first_sub>3 and first_sub<7,'4-7日',
    if(first_sub>=7 and first_sub<14,'7-14日',if(first_sub>=14 and first_sub<30,'14-30日',
    if(first_sub>=30 and first_sub<90,'30-90日','90日以上'))))))))) first_box,
    if(user_sub=0,'当日',if(user_sub=1,'次日',if(user_sub=2,'3日',
    if(user_sub=3,'4日',if(user_sub>3 and user_sub<7,'4-7日',
    if(user_sub>=7 and user_sub<14,'7-14日',if(user_sub>=14 and user_sub<30,'14-30日',
    if(user_sub>=30 and user_sub<90,'30-90日','90日以上')))))))) logon_box,
    book_id,referral_book,admin_id channel_id,type
from (
    SELECT 
        date(FROM_UNIXTIME(createtime)) date_day,
        date(FROM_UNIXTIME(first_time)) first_day,
        TIMESTAMPDIFF(DAY,date(FROM_UNIXTIME(first_time)),date(FROM_UNIXTIME(createtime))) first_sub,
        TIMESTAMPDIFF(DAY,date(FROM_UNIXTIME(user_createtime)),date(FROM_UNIXTIME(createtime))) user_sub,
        book_id,referral_book,admin_id,type,if(createtime=first_time,1,0) is_first,user_id
    from orders_log.orders_log_{num}
    where deduct = 0 and state = 1 and createtime >= UNIX_TIMESTAMP('{s_date}')
) a
"""


sql_user_referral = """
SELECT user_id,referral_id,date(FROM_UNIXTIME(createtime)) logon_day
from user_info.user_info_0
where createtime >= UNIX_TIMESTAMP({s_date})
"""

sql_order_day_logon = """
SELECT user_id,TIMESTAMPDIFF(day, FROM_UNIXTIME(user_createtime), FROM_UNIXTIME(createtime)) day_sub,money
from orders_log.orders_log_{num}
where state = 1 and deduct = 0
"""

sql_referral_info = """
SELECT id referral_id,book_id,admin_id,cost,date(FROM_UNIXTIME(createtime)) referral_day
from market_read.referral_info
"""

sql_referral_logon_user = """
SELECT referral_id,date(FROM_UNIXTIME(createtime)) logon_day,count(*) logon_users
from user_info.user_info_{num}
where referral_id > 0 and createtime >= UNIX_TIMESTAMP({s_date})
GROUP BY referral_id,logon_day
"""


""" ************** -*- one_book_locus -*- ************** """

sql_one_book_logon = """
SELECT user_id,'logon' type,FROM_UNIXTIME(createtime) create_date,date(FROM_UNIXTIME(createtime)) date_day
from user_info.user_info_{num}
where referral_book = {book_id}
"""

sql_one_book_read = """
SELECT user_id,'read' type,
    FROM_UNIXTIME(createtime) create_date,date(FROM_UNIXTIME(createtime)) date_day
from user_read.user_read_{num}
where book_id = {book_id}
"""

sql_one_book_order = """
SELECT user_id,'order' type,
    FROM_UNIXTIME(createtime) create_date,date(FROM_UNIXTIME(createtime)) date_day,money,type order_type
from orders_log.orders_log_{num}
where book_id = {book_id} and state = 1 and deduct= 0
"""

sql_one_book_consume = """
SELECT user_id,'consume' type,FROM_UNIXTIME(createtime) create_date,date(FROM_UNIXTIME(createtime)) date_day
from cps_shard_{num}.consume
where book_id = {book_id}
"""

sql_one_book_user_info = sql_user_info_kd_log

sql_order_today_user_before = """
SELECT book_id,admin_id channel_id,count(*) before_times,count(DISTINCT user_id) before_user,sum(money) before_money
from orders_log.orders_log_{num} 
where user_id in (
    SELECT user_id 
    from orders_log.orders_log_{num} 
    where createtime >= UNIX_TIMESTAMP('{s_date}')
    and createtime < UNIX_TIMESTAMP('{e_date}')
    and book_id = {book}
)
and createtime < UNIX_TIMESTAMP('{s_date}')
GROUP BY book_id,admin_id
"""

sql_book_before_users = """
SELECT book_id,channel_id,count(DISTINCT user_id) book_user
from user_read.user_read_{num}
where createtime < UNIX_TIMESTAMP('{s_date}')
and book_id = {book}
GROUP BY book_id,channel_id
"""

sql_channel_before_users = """
SELECT admin_id channel_id,count(*) admin_user
from user_info.user_info_{num}
where createtime < UNIX_TIMESTAMP('{s_date}')
GROUP BY admin_id
"""

sql_active_sub = """
SELECT channel_id,type,day_sub,count(DISTINCT user_id) active_users
from one_book.book_{book}_{num}
where date_day = '{s_date}'
GROUP BY channel_id,type,day_sub
"""

sql_logon_sub = """
SELECT channel_id,logon_sub,count(DISTINCT user_id) active_users
from one_book.book_{book}_{num}
where date_day = '{s_date}'
and type = 'order'
GROUP BY channel_id,logon_sub
"""

sql_result_order = """
SELECT channel_id,count(DISTINCT user_id) order_users,count(*) order_times
from one_book.book_{book}_{num}
where date_day = '{s_date}' and type = 'order'
GROUP BY channel_id
"""

sql_mid_active_sub = """
SELECT channel_id,type,day_sub,sum(active_users) active_users
from model.mid_active_sub
GROUP BY channel_id,type,day_sub
"""

sql_mid_book_before_users = """
SELECT channel_id,sum(book_user) book_user
from model.mid_book_before_users
GROUP BY channel_id
"""

sql_mid_channel_before_users = """
SELECT channel_id,sum(admin_user) admin_user
from model.mid_channel_before_users
GROUP BY channel_id
"""

sql_mid_logon_sub = """
SELECT channel_id,logon_sub,sum(active_users) active_users
from model.mid_logon_sub
GROUP BY channel_id,logon_sub
"""

sql_mid_result_order = """
SELECT channel_id,sum(order_users) order_users,sum(order_times) order_times
from model.mid_result_order
GROUP BY channel_id
"""

sql_mid_before_order_book = """
SELECT channel_id,book_id,sum(before_times) before_times,sum(before_user) before_user,sum(before_money) before_money
from model.mid_before_order
GROUP BY channel_id,book_id
"""

sql_mid_before_order_money = """
SELECT channel_id,before_money money_box,sum(before_times) money_times
from model.mid_before_order
GROUP BY channel_id,before_money
"""


""" ************** -*- custom sql -*- ************** """

sql_custom = """
SELECT custom_id,date(FROM_UNIXTIME(sendtime)) send_date,title,book_id,'客服消息' tab,
    admin_id channel_id,user_json,send_num,statue,if(message_type=1,'文字信息','图文信息') types
from market_read.custom
"""

sql_mp_send = """
SELECT date(FROM_UNIXTIME(sendtime)) send_date,title,admin_id channel_id,'高级群发' tab,
    user_json,send_num,statue,message_type types,success_num
from market_read.mp_send
"""

sql_templatemessage = """
SELECT date(FROM_UNIXTIME(sendtime)) send_date,title,admin_id channel_id,
    user_json,send_num,statue,success_num,'模板消息' tab
from market_read.templatemessage
"""

sql_custom_url = """
SELECT custom_id,uv,pv,recharge_orders,recharge_money
from market_read.custom_url_collect
"""


sql_one_book_read_consume = """
SELECT user_id,book_id,book_name,chapter_id,chapter_name,kandian,
    free_kandian,createtime,date(FROM_UNIXTIME(createtime)) date_day
from cps_shard_{num}.consume
where book_id = {book_id} and createtime >= UNIX_TIMESTAMP('{s_date}')
"""

sql_one_book_read_read = """
SELECT book_id,user_id,chapter_id,chapter_name,updatetime,createtime,user_type,date(FROM_UNIXTIME(updatetime)) date_day
from cps_shard_{num}.user_recently_read
where book_id = {book_id} and updatetime >= UNIX_TIMESTAMP('{s_date}')
"""


sql_keep_consume = """
SELECT user_id,date(FROM_UNIXTIME(createtime)) date_day,book_id,1 consume_user
from cps_shard_{num}.consume
where createtime >= UNIX_TIMESTAMP('{s_date}')
GROUP BY user_id,date_day,book_id
"""

sql_keep_order = """
SELECT user_id,book_id,date(FROM_UNIXTIME(createtime)) date_day,1 order_users,count(*) order_times,sum(money) moneys
from cps_user_{num}.orders
where state = '1' and deduct = '0' and createtime >= UNIX_TIMESTAMP('{s_date}')
GROUP BY user_id,date_day,book_id
"""

sql_keep_user_info = """
SELECT user_id,referral_book,admin_id,date(FROM_UNIXTIME(createtime)) logon_date
from user_info.user_info_{num}
"""

sql_keep_logon = """
SELECT referral_book book_id,admin_id,date(FROM_UNIXTIME(createtime)) logon_date,count(*) logon_users
from user_info.user_info_{num}
GROUP BY referral_book,admin_id,logon_date
"""

sql_keep_one_day = """
SELECT *
from model_keep.model_keep_{num}
where date_day = '{s_date}'
"""


""" ************** -*- clickhouse -*- ************** """

click_sql_create_table = """
CREATE TABLE heiyan.read_log(
    ip String,
    time DateTime,
    user_id String,
    method String,
    url String,
    status String,
    size Int8,
    site String,
    day String
) ENGINE = MergeTree() 
PRIMARY KEY time 
PARTITION BY toYYYYMMDD(time) 
ORDER BY time; 
"""

click_sql_delete_table_data = """
alter table {db}.{tab} delete where {col} {cd} '{val}'
"""


sql_hy_pv_uv = """
select day,if(book_id='',refer_book,book_id) book_id,count(*) pv,count(distinct user_id) uv
from heiyan.read_log
group by day,book_id
"""

sql_hy_orders = """
select day,book_id,sum(orders) orders,sum(success) success,sum(real_money) real_money,
    count(DISTINCT user_id) order_users,count(DISTINCT success_user) success_user
from (
    select day,book_id,status,real_money,1 orders,if(status=2,1,0) success,user_id,
        if(status=2,user_id,NULL) success_user
    from heiyan.order_log ol
    left join heiyan.order_book ok on ol.id = ok.topup_id
) orders
where day >= '2021-06-01'
group by day,book_id
"""

sql_hy_first_orders = """
select day,book_id,sum(f_r.first) first_user,count(DISTINCT if(f_r.first=0,f_r.user_id,NULL)) reorder_user
from (
    SELECT ol.id,fo.first as `first`,ol.day day,ol.user_id user_id,ok.book_id book_id
    from heiyan.order_log ol
    left join heiyan.order_book ok on ol.id = ok.topup_id
    left join (
        select min(id) id,1 first
        from heiyan.order_log
        where status = 2
        group by user_id
    ) fo on ol.id = fo.id
    where ol.status = 2
) f_r
group by day,book_id
"""

sql_hy_book_info = """
select id book_id,i_name,date(create_time) book_create,words
from heiyan.book_info bi  
"""

sql_hy_user_join_book = """
select user_id,if(book_id='0',refer_book,book_id) book_id,min(day) join_day
from heiyan.read_log rl 
where user_id != '0'
group by user_id,book_id
"""

sql_hy_user_read_log = """
select if(book_id='0',refer_book,book_id) book_id,
    if(chapter_id='0',refer_chapter,chapter_id) chapter_id,type,user_id,plat,day
from heiyan.read_log
where day = '2021_06_01'
"""

sql_hy_chapter_info = """
select id chapter_id,sequence,book_id chapter_book,free
from heiyan.chapter_info 
where update_time > '2021-01-01'
"""

sql_uc_user_id = """
select uc,max(user_id) uid
from (
    select uc,user_id
    from heiyan.read_log 
    where user_id > 0 and uc > ''
    group by uc,user_id
) a
group by uc
"""

sql_chapter_sequence = """
select id chapter_id,`sequence` ,`free` 
from heiyan.chapter_info ci 
where create_time >= '2020-01-01'
"""

sql_restructure_read_log = """
select if(user_id>0,user_id,u.uid) user_id,uc,type,site,day,plat,book_id,chapter_id
from (
    select user_id,uc,site,day,plat,type,
    if(book_id='0',refer_book,book_id) book_id,
    if(chapter_id='0',refer_chapter,chapter_id) chapter_id 
    from heiyan.read_log
    where day = '{date_day}'
) rl 
left join (
    select uc,max(user_id) uid
    from (
        select uc,user_id
        from heiyan.read_log 
        where user_id > 0 and uc > ''
        group by uc,user_id
    ) a
    group by uc
) u on u.uc = rl.uc
"""

sql_restructure_read_log_mid = """
SELECT user_id,uc,if(free='true','1',type) type,site,day,plat,book_id,chapter_id,sequence,free
from heiyan.show_follow_tab sft 
where day = '{date_day}'
"""

sql_restructure_read_first = """
SELECT user_id,book_id,min(day) first_day
from heiyan.show_follow_tab sft 
group by user_id,book_id
"""


sql_restructure_group = """
SELECT first_day,book_id,`sequence`,count(DISTINCT user_id) users
from heiyan.show_follow_tab
where book_id > '' and sequence > 0
group by first_day,book_id,`sequence` 
having sequence <= 90
order by first_day desc,book_id,sequence
"""

sql_book_info_group = """
select id book_id,i_name,if(finish_time<'1970-02-01','连载','完结') finish
from heiyan.book_info
where status >= 0
"""

sql_restructure_pv_uv = """
SELECT first_day,book_id,count(*) pv,count(DISTINCT user_id) uv
from heiyan.show_follow_tab
where book_id > ''
group by first_day,book_id
"""


""" ************** -*- give up sql -*- ************** """

sql_order_users_money_test = """
SELECT date(FROM_UNIXTIME(createtime)) date_day,user_id,type
from cps_user_{num}.orders
where createtime >= UNIX_TIMESTAMP('{s_date}') and state = '1' and deduct = '0'
# group by date_day
"""
