from algorithm import retained


def syn_market_keep_day(s_date=None):
    work = retained.KeepTableDay(list_day=[1, 2, 3, 7, 14, 30])
    if s_date:
        work.s_date = s_date
    work.count_keep_table_day_run()


def syn_admin_book_order():
    work = retained.RunCount('market_read', 'order_logon_conversion', 'order_day', extend='delete')
    work.step_run(retained.count_order_logon_conversion)
    work.direct_run(retained.compress_order_logon_conversion)


if __name__ == '__main__':
    print('Start work:')
    syn_market_keep_day()
    syn_admin_book_order()
