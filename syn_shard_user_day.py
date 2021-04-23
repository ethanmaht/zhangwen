from emtools import read_database as rd
from emtools import data_job
from es_worker import es_tool
from logs import loger
from algorithm import retained


@loger.logging_read
def syn_shard_user_day_work(s_date=None):
    shard_user_day_work = rd.DataBaseWork()
    if s_date:
        shard_user_day_work.date = s_date
    shard_user_day_work.date_sub = 7
    shard_user_day_work.loop_all_database()


@loger.logging_read
def syn_date_block(s_date=None):
    rd.syn_date_block_run(
        data_job.read_kd_log, s_date, process_num=8,
        write_conn_fig='datamarket', write_db='log_block', write_tab='action_log'
    )


@loger.logging_read
def syn_read_user_recently_read(s_date=None):
    rd.syn_date_block_run(
        data_job.read_user_recently_read, s_date, process_num=8,
        write_conn_fig='datamarket', write_db='user_read', write_tab='user_read'
    )


@loger.logging_read
def syn_read_sign_order_count(s_date=None):
    rd.syn_date_block_run(
        data_job.read_sign_order_read, s_date, process_num=12,
        write_conn_fig='datamarket', write_db='market_read', write_tab='sign_order_count'
    )


@loger.logging_read
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


@loger.logging_read
def syn_portrait_user_order_run(s_date=None):
    data_job.delete_portrait_user_order(
        write_conn_fig='datamarket', write_db='market_read', write_tab='portrait_user_order'
    )
    rd.syn_date_block_run(
        data_job.portrait_user_order_run, s_date, process_num=16,
        write_conn_fig='datamarket', write_db='market_read', write_tab='portrait_user_order'
    )


@loger.logging_read
def syn_portrait_user_order_admin_book_run(s_date=None):
    data_job.delete_portrait_user_order(
        write_conn_fig='datamarket', write_db='market_read', write_tab='portrait_user_order_admin_book'
    )
    rd.syn_date_block_run(
        data_job.portrait_user_order_run_admin_book, s_date, process_num=12,
        write_conn_fig='datamarket', write_db='market_read', write_tab='portrait_user_order_admin_book'
    )
    data_job.portrait_user_order_run_admin_book_count(
        write_conn_fig='datamarket', write_db='market_read', write_tab='portrait_user_order_admin_book'
    )


def syn_user_date_interval_run(s_date=None):
    work = retained.RunCount(
        write_db='user_interval', write_tab='user_date_interval', date_col='date_day', extend='delete'
    )
    if s_date:
        work.s_date = s_date
    work.step_run_kwargs(
        func=data_job.user_date_interval,
        date_sub=30,
        process_num=8
    )


if __name__ == '__main__':
    print('Start work:')
    """ ****** ↓ 自动并部署 ↓ ****** """
    syn_shard_user_day_work()  # 基础数据同步 -> .5h
    syn_date_block()  # 动作日志分时间块记录 -> 1.5h
    syn_read_user_recently_read('2021-01-01')  # 跟读数据同步
    syn_happy_seven_sound_shard_work('2020-01-1')  # 有声的数据库同步 -> .1h
    es_tool.run_read_ex_loop(
        sub_days=5, size=10000, write_conn_fig='datamarket', write_db='sound', tab='es_log'
    )  # 有声的es同步 -> .1h
    syn_portrait_user_order_run()  # 用户画像数据同步 -> .3h
    syn_portrait_user_order_admin_book_run()  # 用户画像-渠道-书 数据同步 -> .7h

    syn_user_date_interval_run()  # 活跃间隔同步 -> .5h

    """ ****** ↓ 手动或者测试 ↓ ****** """
    # syn_read_sign_order_count()  # 连续签到奖励查询 - 手动: 李迪
    # es_tool.run_read_ex_loop(
    #     sub_days=1, size=10000, write_conn_fig='datamarket', write_db='sound', tab='es_log',
    #     index='logstash-qiyue-accesslog*'
    # )  # 七悦的es同步_测试
