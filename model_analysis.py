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


if __name__ == '__main__':
    syn_index_book_consume_run(s_date='2019-01-01')
