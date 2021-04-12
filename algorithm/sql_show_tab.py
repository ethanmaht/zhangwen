
sql_logon_admin_book_val = """
SELECT logon_day 激活日期,order_day 充值日期,base.admin_id,admin.nickname 公众号,
    admin.business_name 商务,base.book_id,concat(book.name, ' ', book.id) 书名,date_sub,
    if(date_sub=0,'首日', 
        if(date_sub=1,'次日', 
            if(date_sub=2,'三日', 
                if((date_sub>=3) & (date_sub<7),'四至七日', '七日以上')))) box,
    logon_user 激活,first_order_user 首充_用户,first_order_money 首充_金额,
    repeat_order_user 复充_用户,repeat_order_times 复充_笔,repeat_order_money 复充_金额,
        first_repeat_order_user 首次_复充,
    vip_order_user vip用户,vip_order_money vip金额
FROM market_read.order_logon_conversion base
left join market_read.book_info book on book.id = base.book_id
left join market_read.admin_info admin on admin.id = base.admin_id
where logon_day >= '{date}'
"""
