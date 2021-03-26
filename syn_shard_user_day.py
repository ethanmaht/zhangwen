from emtools import read_database as rd
from emtools import data_job


def syn_shard_user_day_work(s_date=None):
    shard_user_day_work = rd.DataBaseWork()
    if s_date:
        shard_user_day_work.date = s_date
    shard_user_day_work.loop_all_database()


def syn_date_block(s_date=None):
    rd.syn_date_block_run(
            data_job.read_kd_log, s_date, process_num=8,
            write_conn_fig='datamarket', write_db='log_block', write_tab='action_log'
        )


if __name__ == '__main__':
    syn_shard_user_day_work('2021-03-16')
    syn_date_block()
