from clickhouse_driver import Client
from emtools import read_database as rd
from emtools import sql_code
import pandas as pd


def id_to_int(id_col):
    if id_col:
        return int(str(id_col).replace('.', ''))
    return 0


def hy_show_tab_conversion(conn, db_name, tab_name):
    pv_uv = rd.read_click_sql(sql_code.sql_hy_pv_uv, conn)
    orders = rd.read_click_sql(sql_code.sql_hy_orders, conn)
    first = rd.read_click_sql(sql_code.sql_hy_first_orders, conn)
    book = rd.read_click_sql(sql_code.sql_hy_book_info, conn)

    pv_uv[['day', 'book_id']] = pv_uv[['day', 'book_id']].astype(str)
    orders[['day', 'book_id']] = orders[['day', 'book_id']].astype(str)
    first[['day', 'book_id']] = first[['day', 'book_id']].astype(str)
    book['book_id'] = book['book_id'].astype(str)

    book['i_name'] = [str(_).replace("'", "_") for _ in book['i_name']]

    pv_uv['day'] = [str(_).replace('_', '-') for _ in pv_uv['day']]
    pv_uv['book_id'] = [str(_).replace('.', '') for _ in pv_uv['book_id']]

    all_data = pd.merge(pv_uv, orders, on=['day', 'book_id'], how='left')
    all_data = pd.merge(all_data, first, on=['day', 'book_id'], how='left')
    all_data = pd.merge(all_data, book, on=['book_id'], how='left')
    all_data.fillna(0, inplace=True)

    all_data[
        ['pv', 'uv', 'orders', 'success', 'order_users', 'first_user', 'reorder_user', 'success_user', 'words']
    ] = \
        all_data[
            ['pv', 'uv', 'orders', 'success', 'order_users', 'first_user', 'reorder_user', 'success_user', 'words']
        ].astype(int)

    print(all_data.columns, all_data.index.size)
    # delete_sql = sql_code.click_sql_delete_table_data.format(
    #     db=db_name, tab=tab_name, col='day', cd='>=', val='2021-06-01'
    # )
    # rd.execute_click_sql(delete_sql, conn)
    # rd.write_click_date(all_data, conn, db_name, tab_name, step=10000)

    conn_mysql = rd.connect_database_vpn('datamarket_out')
    rd.delete_table_data(conn_mysql, db_name, tab_name)
    rd.insert_to_data(all_data, conn_mysql, db_name, tab_name)
    conn_mysql.close()


def hy_show_tab_keep(conn, db_name, tab_name):
    join_user = rd.read_click_sql(sql_code.sql_hy_user_join_book, conn)
    print(1)
    read_log = rd.read_click_sql(sql_code.sql_hy_user_read_log, conn)
    print(2)
    chapter_info = rd.read_click_sql(sql_code.sql_hy_chapter_info, conn)

    join_user[['user_id', 'book_id']] = join_user[['user_id', 'book_id']].astype(str)
    read_log[['user_id', 'chapter_id', 'book_id']] = read_log[['user_id', 'chapter_id', 'book_id']].astype(str)
    chapter_info[['chapter_id', 'chapter_book']] = chapter_info[['chapter_id', 'chapter_book']].astype(str)

    all_data = pd.merge(read_log, join_user, on=['user_id', 'book_id'], how='left')
    all_data = pd.merge(all_data, chapter_info, on=['chapter_id'], how='left')

    all_data.fillna(0, inplace=True)
    all_data['join_day'] = [str(_).replace('_', '-') for _ in all_data['join_day']]
    all_data['day'] = [str(_).replace('_', '-') for _ in all_data['day']]

    delete_sql = sql_code.click_sql_delete_table_data.format(
        db=db_name, tab=tab_name, col='join_day', cd='>=', val='2021-06-01'
    )

    print(all_data.dtypes)
    rd.execute_click_sql(delete_sql, conn)
    rd.write_click_date(all_data, conn, db_name, tab_name, step=10000)


if __name__ == '__main__':
    client = Client(host='127.0.0.1', user='testuser', password='1a2s3d4f', database='heiyan')

    hy_show_tab_conversion(client, 'heiyan', 'show_tab_conversion')

    # hy_show_tab_keep(client, 'heiyan', 'mid_read_log')
