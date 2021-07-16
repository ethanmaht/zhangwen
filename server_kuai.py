import assignment_pool as ap
from es_worker import es_tool


if __name__ == "__main__":
    ap.syn_happy_seven_sound_shard_work('2021-06-01')  # 有声的数据库同步 -> .1h
    es_tool.run_read_ex_loop(
        sub_days=2, size=10000, write_conn_fig='datamarket_out', write_db='sound', tab='es_log'
    )  # 有声的es同步 -> .1h
    ap.referral_roi_run(s_date='2021-01-01')  # 推广roi
    ap.referral_roi_90_run(s_date='2021-01-01')

