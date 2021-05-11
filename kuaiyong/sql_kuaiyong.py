
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

