
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
SELECT date(FROM_UNIXTIME(createtime)) date_day,'logon' type,user_id,book_id,channel_id,1 nums
from log_block.action_log{block}_{num}
where type=0 and createtime >= UNIX_TIMESTAMP('{s_date}')
GROUP BY date_day,user_id,book_id,channel_id
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
    money,money_benefit,benefit,kandian,free_kandian,user_createtime,deduct
FROM cps_user_{_num}.orders
where createtime >= '{date}'
"""

sql_user_info = """
select id user_id,createtime,updatetime,channel_id admin_id,sex,country,is_subscribe,
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
    last_chapter_id,read_num,is_cp,cp_name,book_recharge,createtime,updatetime,chapter_num
FROM cps.book;
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

sql_recently_cread_data = """
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
        SELECT a.referral_book book_id,a.channel_id,b.is_finish,a.createtime,a.updatetime,book_create,logon_date,
            CAST(if(a.channel_free_chapter_num<>0,a.channel_free_chapter_num,
            if(a.free_chapter_num<>0,a.free_chapter_num,15)) AS SIGNED) free_chapter,
            CAST(b.chapter_num AS SIGNED) last_chapter_id,CAST(chapter_id AS SIGNED) chapter_id
        from user_read.user_read_{num} a
        left join market_read.book_info b on b.id = a.referral_book
        where a.createtime >= UNIX_TIMESTAMP('{date}')
    ) base
) box
GROUP BY book_id,channel_id,last_chapter_id,is_finish,start_date
"""

sql_book_admin_read_count = """
SELECT book_id,channel_id,last_chapter_id,is_finish,start_date,sum(start_book) start_book,
    sum(over_free) over_free,sum(over_free) / count(*) over_free_p, 
    sum(over_100) over_100,sum(over_100) / count(*) over_100_p, 
    sum(over_200) over_200, sum(over_200) / count(*) over_200_p, 
    sum(over_300) over_300, sum(over_300) / count(*) over_300_p, 
    sum(over_500) over_500, sum(over_500) / count(*) over_500_p, 
    sum(over_750) over_750, sum(over_750) / count(*) over_750_p,
    sum(over_1000) over_1000,sum(over_1000) / count(*) over_1000_p, 
    sum(over_2000) over_2000,sum(over_2000) / count(*) over_2000_p, 
    sum(over_book) over_book,sum(over_book) / count(*) over_book_p
from {db}.{tab}
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
SELECT date(FROM_UNIXTIME(createtime)) logon_day,user_id,1 logon_user,is_subscribe,referral_id_permanent
from sound.`user` 
where createtime >= UNIX_TIMESTAMP('{s_date}')
"""

sount_order_log = """
SELECT user_id,count(*) order_times,sum(money) money,1 order_users
from sound.orders
where state = 1 and benefit = 0 and createtime >= UNIX_TIMESTAMP('{s_date}')
GROUP BY user_id
"""

sound_referral_info = """
SELECT r.id referral_id_permanent,r.admin_id,a.business_name,a.nickname,b.`name` logon_book,c.channel_free_chapter_num
from sound.referral r
left join sound.admin a on a.id = r.admin_id
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
    SELECT date(time) date_day,l.book_id,l.chapter_id,pe.chapter_id chapter_num,
        l.admin_id,count(*) pv,count(DISTINCT uid) uv
    from sound.es_log l
    left join sound.podcasts p on l.book_id = p.id
    left join sound.podcast_episodes pe on p.origin_id = pe.book_id and l.chapter_id = pe.id
    where l.book_id > 0 and l.chapter_id > 0 and time > '{s_date}'
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


"""
************** -*- give up sql -*- **************
"""

analysis_reason_for_save = """
select count(*) nums,sum(bv) saves,sum(signv) reason_signs,sum(fdv) reason_fd,
    sum(kdv) reason_dk,sum(orderv) reason_order, 'all' types
from (
    SELECT distinct a.user_id, bv,signv,fdv,kdv,orderv FROM happy_seven.user_day_{tab_num} a
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
