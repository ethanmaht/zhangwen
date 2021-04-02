import pandas as pd
from emtools import read_database as rd
from emtools import sql_code


def sound_book_admin_count(read_config, write_db, write_tab, date_col, s_date):
    conn = rd.connect_database_host(read_config['host'], read_config['user'], read_config['pw'])
    user = pd.read_sql(
        sql_code.sound_user_log.format(s_date=s_date), conn
    )
    order = pd.read_sql(
        sql_code.sount_order_log.format(s_date=s_date), conn
    )
    referral = pd.read_sql(
        sql_code.sound_referral_info.format(s_date=s_date), conn
    )
    keep = pd.read_sql(sql_code.sound_keep.format(s_date=s_date), conn)
    user['user_id'] = user['user_id'].astype(int)
    user['referral_id_permanent'] = user['referral_id_permanent'].astype(int)

    order['user_id'] = order['user_id'].astype(int)
    referral['referral_id_permanent'] = referral['referral_id_permanent'].astype(int)

    keep['user_id'] = keep['user_id'].astype(int)

    sound_data = pd.merge(user, referral, on='referral_id_permanent', how='left')
    sound_data = pd.merge(sound_data, order, on='user_id', how='left')
    sound_data = pd.merge(sound_data, keep, on=['user_id', 'logon_day'], how='left')

    sound_data['is_subscribe'] = sound_data['is_subscribe'].astype(int)
    sound_data = sound_data.fillna(0)
    sound_data = sound_data.groupby(
        by=['logon_day', 'business_name', 'nickname', 'logon_book', 'channel_free_chapter_num']
    )['is_subscribe', 'logon_user', 'order_users', 'order_times', 'keep'].sum().reset_index()
    sound_data = sound_data.fillna(0)

    rd.insert_to_data(sound_data, conn, write_db, write_tab)
    conn.close()


def sound_chapter_admin_count(read_config, write_db, write_tab, date_col, s_date):
    conn = rd.connect_database_host(read_config['host'], read_config['user'], read_config['pw'])
    data = pd.read_sql(
        sql_code.sound_chapter_pv_uv.format(s_date=s_date), conn
    )
    data = data.fillna(0)
    rd.insert_to_data(data, conn, write_db, write_tab)
    conn.close()
