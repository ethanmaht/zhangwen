from emtools import emdate
import pandas as pd
from pandas import DataFrame as df
from elasticsearch import Elasticsearch
from emtools import read_database as rd


def connect_es(sub_day, index, host='http://192.168.1.221', size=10000, s_num=0):
    es_host = host
    es = Elasticsearch(hosts=es_host, port=9200, timeout=15000)
    body = {
        "query":
            {
                "bool":
                    {
                        "must": [
                            {
                                'range':
                                    {
                                        '@timestamp': {'gte': u'now-{days}d'.format(days=sub_day), 'lte': 'now'}
                                    }
                            }
                        ],
                        "must_not": [],
                        "should": []
                    }
            },
        "from": s_num, "size": size, "sort": [], "aggs": {}
    }
    res = es.search(index=index, body=body)
    data = res['hits']['hits']
    data = draw_date_from_es_to_df(data)
    return data


def draw_date_from_es_to_df(one_day, ):
    dict_pool = []
    for _one in one_day:
        body = _one['_source']
        try:
            if body['tag'] != 701:
                continue
        except:
            continue
        try:
            user_from = body['map']['from']
        except:
            user_from = '0'
        try:
            book_id = body['map']['book_id']
        except:
            book_id = '0'
        try:
            chapter_id = body['map']['chapter_id']
        except:
            chapter_id = '0'
        df_data = {
            'admin_id': body['admin_id'],
            'page': body['page'],
            'channel_id': body['channel_id'],
            'referral_id': body['referral_id'],
            'time': body['time'],
            'uid': body['uid'],
            'user_from': user_from,
            'book_id': book_id,
            'chapter_id': chapter_id,
        }
        dict_pool.append(df_data)
    return df(dict_pool)


def run_read_ex_loop(sub_days, size, write_conn_fig, write_db, tab, index="logstash-qiyue-sound-access*"):
    start_page, all_data = 0, []
    re_data = connect_es(sub_days, size=size, s_num=start_page, index=index)
    _size = re_data.index.size
    all_data.append(re_data)
    while _size >= size:
        start_page += size
        re_data = connect_es(sub_days, size=size, s_num=start_page)
        _size = re_data.index.size
        all_data.append(re_data)
    all_data = pd.concat(all_data)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    _sub_date = emdate.date_sub_days(sub_days)
    rd.delete_last_date(write_conn, write_db, tab, date_type_name='time', date=_sub_date)
    rd.insert_to_data(all_data, write_conn, write_db, tab)
