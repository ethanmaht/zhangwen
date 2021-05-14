
sql_user_in_channel = """
SELECT user_id,ctime,date(ctime) create_date,channel_code,extend
from glory_sharding_{num}.u_user_info
where channel_code in ('XJTT-AAB1000000', 'XJTT-AAB1000001','XJTT-AAB1000002')
"""

sql_recently_read = """
SELECT uid user_id,bookid book_id,chapterid,ctime read_time,date(ctime) read_date,chapterid % 10000 chapter_num
from glory_sharding_{num}.b_recently_read
"""

sql_consume = """
SELECT user_id,(chapter_id div 100000) book_id,sum(amount) amount,sum(award) award
from glory_sharding_{num}.b_owch_consume_h
GROUP BY user_id,book_id
"""

sql_orders = """
SELECT user_id,if(status_notify=1,1,0) orders,1 s_order,extend
from glory_userdata.b_order 
where ctime >= '{s_date}'
"""

sql_admin_info = """
SELECT r.id,channel_code,re.package_code,r.business_id,bu.nickname business,
    r.optimizer_id,op.nickname optimizer,r.vip_id,vip.nickname vip
from kyycps.referral_channel_relation r
left join kyycps.admin bu on r.business_id = bu.id
left join kyycps.admin op on r.optimizer_id = op.id
left join kyycps.admin vip on r.vip_id = vip.id
left join kyycps.referral re on r.referral_id = re.id
"""

sql_user_info = """
SELECT user_id,ctime,date(ctime) create_date,channel_code,extend
from glory_sharding_{num}.u_user_info
"""

sql_user_order = """
SELECT user_id,ctime,status_notify,extend
from glory_userdata.b_order
where ctime >= '{s_date}'
"""

sql_user_first_order = """
SELECT user_id,min(ctime) first_time
from glory_userdata.b_order
where status_notify=1
GROUP BY user_id
"""

sql_book_free_chapter = """
SELECT book_id,count(*) free_num
from glory.b_book_chapter
where is_free = 0
GROUP by book_id
"""

sql_consume_log = """
SELECT user_id,type,src_id book_id,chapter_id,channel_code,amount,award,ctime
from glory_sharding_{num}.b_owch_consume_h
where ctime >= '{num}'
"""
