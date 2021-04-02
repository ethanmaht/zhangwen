from emtools import read_database as rd
from emtools import data_job
from es_worker import es_tool


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


def syn_read_user_recently_read(s_date=None):
    rd.syn_date_block_run(
            data_job.read_user_recently_read, s_date, process_num=8,
            write_conn_fig='datamarket', write_db='user_read', write_tab='user_read'
        )


def syn_happy_seven_sound_shard_work(s_date=None):
    rd.syn_date_block_run(
            func=data_job.syn_happy_seven_sound_shard, date=s_date, process_num=8,
            write_conn_fig='datamarket', write_db='sound',
            write_tab_list=['user', 'orders']
        )
    rd.syn_date_block_run(
            func=data_job.syn_happy_seven_sound_shard_num, date=s_date, process_num=8,
            write_conn_fig='datamarket', write_db='sound',
            write_tab_list=['consume', 'sign']
        )
    data_job.syn_happy_seven_sound_cps('datamarket', 'sound')


if __name__ == '__main__':
    syn_shard_user_day_work()  # 基础数据同步
    syn_date_block()  # 动作日志分时间块记录
    syn_read_user_recently_read()  # 跟读数据同步
    syn_happy_seven_sound_shard_work('2020-01-1')  # 有声的数据库同步
    es_tool.run_read_ex_loop(
        sub_days=3, size=10000, write_conn_fig='datamarket', write_db='sound', tab='es_log'
    )  # 有声的es同步
