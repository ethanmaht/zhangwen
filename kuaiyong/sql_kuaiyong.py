
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

sql_book_info = """
SELECT book_id,name book_name,status,price,total_chapter_num,total_word_size,market_status,is_sync
from glory.b_book
"""

sql_user_info = """
SELECT user_id,ctime,utime,date(ctime) create_date,channel_code,extend
from glory_sharding_{num}.u_user_info
where utime >= '{s_date}'
"""

sql_user_order = """
SELECT id,user_id,ctime,status_notify,extend,type,price,discount,award,quantity
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
where ctime >= '{s_date}'
"""

sql_recently_read_info = """
SELECT uid user_id,bookid book_id,chapterid,utime,ctime read_time,date(ctime) read_date,date(utime) u_date,
    chapterid % 10000 chapter_num
from glory_sharding_{num}.b_recently_read
where utime >= '{s_date}'
"""


""" kuaiyong data market sql """

sql_user = """
SELECT user_id,create_date,channel_code,book_id,1 logon_user,isAddDesktop AddDesktop
from kuaiyong.user_info
"""

sql_read = """
SELECT user_id,book_id,chapter_num
from kuaiyong.recently_read
where chapter_num != 0
"""

sql_read_num_sum = """
SELECT read_date,user_id,book_id book,1 nums
from kuaiyong.recently_read
where chapter_num >= {num}
"""

sql_free_num = """
SELECT book_id,free_num
from kuaiyong.free_chapter
"""

sql_first_order = """
SELECT user_id,book_id,1 first_order
from kuaiyong.order_log
where first_time = ctime and status_notify = 1 
"""

sql_re_order = """
SELECT user_id,book_id,1 re_order
from kuaiyong.order_log
where first_time != ctime and status_notify = 1
GROUP BY user_id,book_id
"""

sql_all_order = """
SELECT user_id,book_id,1 all_order
from kuaiyong.order_log
GROUP BY user_id,book_id
"""

sql_admin = """
SELECT channel_code,optimizer,vip,package_code,business
from kuaiyong.admin_info
"""

sql_book = """
SELECT book_id,book_name
from kuaiyong.book_info
"""
