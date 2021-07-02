from emtools import emdate
import pandas as pd
from pandas import DataFrame as df
from elasticsearch import Elasticsearch
from emtools import read_database as rd


def connect_es(sub_day, index, host='http://es-cn-7mz280y590006czc7.elasticsearch.aliyuncs.com', size=10000, s_num=0):
    es_host = host
    es = Elasticsearch(hosts=es_host, port=9200, timeout=15000, http_auth="elastic:dDE4pwJdqnNHg0fH")
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
    data_size = len(data)
    data = draw_date_from_es_to_df(data)
    return data, data_size


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
        try:
            sid = body['map']['sid']
        except:
            sid = '0'
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
            'sid': sid,
        }
        dict_pool.append(df_data)
    return df(dict_pool)


def run_read_ex_loop(sub_days, size, write_conn_fig, write_db, tab, index="logstash-qiyue-sound-access*"):
    start_page, all_data = 0, []
    print('es: 0')
    re_data, _size = connect_es(sub_days, size=size, s_num=start_page, index=index)
    all_data.append(re_data)
    while _size >= size:
        print(start_page)
        start_page += size
        re_data, _size = connect_es(sub_days, index=index, size=size, s_num=start_page)
        all_data.append(re_data)
    all_data = pd.concat(all_data)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    _sub_date = emdate.date_sub_days(sub_days)
    rd.delete_last_date(write_conn, write_db, tab, date_type_name='time', date=_sub_date)
    rd.insert_to_data(all_data, write_conn, write_db, tab)


def es_data_read(
        index,
        ranges_must=None, ranges_must_not=None, ranges_should=None,
        host='http://es-cn-7mz280y590006czc7.elasticsearch.aliyuncs.com', size=10000, s_num=0
):
    if not ranges_must:
        ranges_must = []
    if not ranges_must_not:
        ranges_must_not = []
    if not ranges_should:
        ranges_should = []
    es = Elasticsearch(hosts=host, port=9200, timeout=15000, user='elastic', password='dDE4pwJdqnNHg0fH')
    body = {
        "query":
            {
                "bool":
                    {
                        "must": ranges_must,
                        "must_not": ranges_must_not,
                        "should": ranges_should
                    }
            },
        "from": s_num, "size": size, "sort": [], "aggs": {}
    }
    res = es.search(index=index, body=body)
    data = res['hits']['hits']
    return draw_date_from_es_to_df(data), len(data)


def run_read_one_book_loop(sub_days, size, write_conn_fig, write_db, tab, index="logstash-qiyue-access*"):
    start_page, all_data = 0, []
    ranges_must = [
        {"match": {"page": "/index/book/chapter"}},
        {"match": {"map.book_id": "77522"}},
        {'range': {'@timestamp': {'gte': u'now-{days}d'.format(days=sub_days), 'lte': 'now'}}}
    ]
    ranges_should = [
        # {"match": {"map.book_id": "10078300"}},
        # {"match": {"map.book_id": "78300"}},
    ]
    re_data, _size = es_data_read(
        index=index, size=size, s_num=start_page, ranges_must=ranges_must, ranges_should=ranges_should
    )
    all_data.append(re_data)
    while _size >= size:
        start_page += size
        print(start_page)
        re_data, _size = es_data_read(
            index=index, size=size, s_num=start_page, ranges_must=ranges_must, ranges_should=ranges_should
        )
        all_data.append(re_data)
    all_data = pd.concat(all_data)
    write_conn = rd.connect_database_vpn(write_conn_fig)
    _sub_date = emdate.date_sub_days(sub_days)
    rd.delete_last_date(write_conn, write_db, tab, date_type_name='time', date=_sub_date)
    rd.insert_to_data(all_data, write_conn, write_db, tab)


# run_read_one_book_loop(
#     sub_days=300, size=10000, write_conn_fig='datamarket', write_db='one_book_read', tab='read_log_77522'
# )
