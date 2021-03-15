
sql_logon_admin_book_val = """
SELECT logon_day 激活日期,order_day 充值日期,base.admin_id,admin.nickname 公众号,
    admin.business_name 商务,base.book_id,book.name 书名,date_sub,
    if(date_sub=0,'首日', 
        if(date_sub=1,'次日', 
            if(date_sub=2,'三日', 
                if((date_sub>=3) & (date_sub<7),'四至七日', '七日以上')))) box,
    if(order_type=0,order_user,0) 激活,
    if(order_type=1,order_user,0) 首充_用户,if(order_type=1,order_money,0) 首充_金额,
    if(order_type=2,order_user,0) 复充_用户,if(order_type=2,order_money,0) 复充_金额,
    order_vip vip用户,vip_money vip金额
FROM market_read.order_logon_conversion base
left join market_read.book_info book on book.id = base.book_id
left join market_read.admin_info admin on admin.id = base.admin_id
where logon_day >= '{date}'
"""
