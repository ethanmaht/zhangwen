from algorithm import retained
from algorithm import models


def syn_index_book_consume_run(s_date=None):
    work = retained.RunCount(
        write_db='one_book', write_tab='book_10067828', date_col='date_day',  # extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=models.one_book_locus,
        date_sub=2,
        process_num=4,
        book_id=10067828
    )


def syn_book_mid_data_run(s_date=None):
    work = retained.RunCount(
        write_db='model', write_tab='mid', date_col='date_day',
        extend='list',
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        front_func=models.delete_book_locus_book_mid,
        func=models.book_locus_day,
        follow_func=models.compute_modal_one_day_data,
        date_sub=0,
        process_num=12,
        book_id=10067828,
        tab_list=[
            'mid_active_sub', 'mid_before_order', 'mid_book_before_users', 'mid_channel_before_users',
            'mid_logon_sub', 'mid_result_order'
        ],
    )


if __name__ == '__main__':
    # syn_index_book_consume_run(s_date='2019-01-01')
    syn_book_mid_data_run(s_date='2020-01-20')
