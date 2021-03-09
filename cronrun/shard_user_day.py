from emtools import read_database as rd


def syn_shard_user_day_work(s_date=None):
    shard_user_day_work = rd.DataBaseWork()
    if s_date:
        shard_user_day_work.date = s_date
    shard_user_day_work.loop_all_database()


if __name__ == '__main__':
    syn_shard_user_day_work()
