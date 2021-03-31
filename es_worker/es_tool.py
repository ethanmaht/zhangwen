import logging
from emtools import currency_means as cm
import threading
import time
from emtools import read_database
from algorithm import retained
from pandas import DataFrame as df
from elasticsearch import Elasticsearch


def connect_es():
    # 连接es时host只写ip
    # es_host_list = ['http://192.168.1.220', 'http://192.168.1.221', 'http://192.168.1.222']
    es_host = 'http://192.168.1.221'
    es = Elasticsearch(hosts=es_host, port=9200, timeout=15000)
    body = {
        "query":
            {
                "bool":
                    {
                        "must": [
                            # {
                            #     'match':
                            #         {
                            #             'page': '/index/user/recent'
                            #         },
                            # },
                            {
                                'range':
                                    {
                                        'ext.push_time': {'gte': 100, 'lte': 1000000000000}
                                    }
                            }
                        ],
                        "must_not": [],
                        "should": []
                    }
            },
        "from": 0,
        "size": 10,
        "sort": [],
        "aggs": {}
    }
    res = es.search(index="logstash-qiyue-sound-access*", body=body)
    print(res)
    data = res['hits']['hits']
    print(data)


connect_es()

a = [
    {'_index': 'logstash-qiyue-sound-accesslog-2021.01', '_type': '_doc', '_id': 'gABFGXcBxbLrQxJeM0mg', '_score': 5.8846154,
     '_source':
         {'tag': 701, 'os': 2, 'track': {}, 'ip': '36.112.33.135', 'admin_id': 14616, 'agent_id': 0, '@version': '1',
          'page': '/index/user/recent', 'path': '/data/h5cps_heiyan/cps_access_2021-01-19_14.log',
          'host': 'wx5853600fe5ce9c5b.pinduzw.net', 'theme': 'boy', 'ctx': 1, 'vt': '', 'referral_id': 671352,
          'map': {'sex': '1', 'type': '0'}, 'channel_id': 14616,
          'ext': {'mark': '1023', 'push_idx': 3, 'push_id': '111', 'push_time': 1609397300},
          'network': '4g', 'uid': 8, '@timestamp': '2021-01-19T06:11:41.493Z', 'time': '2021-01-19 14:11:41',
          'timestamp': '2021-01-19T06:11:41.000Z', 'method': 'GET', 'openid': 'opDkt5yqijN2UZ6iFZjoS9VhBc_w'}},
    {'_index': 'logstash-qiyue-sound-accesslog-2021.01', '_type': '_doc', '_id': 'j3ZXGXcBnWP96uv9pSDG', '_score': 5.8846154, '_source': {'tag': 701, 'os': 2, 'track': {}, 'ip': '36.112.33.135', 'admin_id': 14619, 'agent_id': 0, '@version': '1', 'page': '/index/user/recent', 'path': '/data/h5cps_heiyan/cps_access_2021-01-19_14.log', 'host': 'wxd7fcaa19881b3a65.qiyuept.com', 'theme': 'default', 'ctx': 1, 'vt': '', 'referral_id': 671372, 'map': {'sex': '1', 'type': '1'}, 'channel_id': 14619, 'ext': {'mark': '1023', 'push_idx': 2, 'push_id': '111', 'push_time': 1609336710}, 'network': '4g', 'uid': 10, '@timestamp': '2021-01-19T06:31:50.363Z', 'time': '2021-01-19 14:31:49', 'timestamp': '2021-01-19T06:31:49.000Z', 'method': 'GET', 'openid': 'oqhu86meMbAG3c4eQeMpp8NB35j0'}}, {'_index': 'logstash-qiyue-sound-accesslog-2021.01', '_type': '_doc', '_id': 'nMhXGXcBMuWH_P61pcDH', '_score': 5.8846154, '_source': {'tag': 701, 'os': 2, 'track': {}, 'ip': '36.112.33.135', 'admin_id': 14619, 'agent_id': 0, '@version': '1', 'page': '/index/user/recent', 'path': '/data/h5cps_heiyan/cps_access_2021-01-19_14.log', 'host': 'wxd7fcaa19881b3a65.qiyuept.com', 'theme': 'default', 'ctx': 1, 'vt': '', 'referral_id': 671372, 'map': {'sex': '1', 'type': '0'}, 'channel_id': 14619, 'ext': {'mark': '1023', 'push_idx': 2, 'push_id': '111', 'push_time': 1609336710}, 'network': '4g', 'uid': 10, '@timestamp': '2021-01-19T06:31:50.363Z', 'time': '2021-01-19 14:31:50', 'timestamp': '2021-01-19T06:31:50.000Z', 'method': 'GET', 'openid': 'oqhu86meMbAG3c4eQeMpp8NB35j0'}}, {'_index': 'logstash-qiyue-sound-accesslog-2021.01', '_type': '_doc', '_id': 'dnZhGXcBnWP96uv96G2W', '_score': 5.8846154, '_source': {'tag': 701, 'os': 2, 'track': {}, 'ip': '36.112.33.135', 'admin_id': 14619, 'agent_id': 0, '@version': '1', 'page': '/index/user/recent', 'path': '/data/h5cps_heiyan/cps_access_2021-01-19_14.log', 'host': 'wxd7fcaa19881b3a65.qiyuept.com', 'theme': 'default', 'ctx': 1, 'vt': '', 'referral_id': 671372, 'map': {'type': '0', 'from': 'wechat'}, 'channel_id': 14619, 'ext': {'mark': 3201, 'push_idx': 1, 'push_id': '338081', 'push_time': 1610692999}, 'network': '4g', 'uid': 10, '@timestamp': '2021-01-19T06:43:02.829Z', 'time': '2021-01-19 14:43:02', 'timestamp': '2021-01-19T06:43:02.000Z', 'method': 'GET', 'openid': 'oqhu86meMbAG3c4eQeMpp8NB35j0'}}, {'_index': 'logstash-qiyue-sound-accesslog-2021.01', '_type': '_doc', '_id': '8MhhGXcBMuWH_P611Pb-', '_score': 5.8846154, '_source': {'tag': 701, 'os': 2, 'track': {}, 'ip': '36.112.33.135', 'admin_id': 14619, 'agent_id': 0, '@version': '1', 'page': '/index/user/recent', 'path': '/data/h5cps_heiyan/cps_access_2021-01-19_14.log', 'host': 'wxd7fcaa19881b3a65.qiyuept.com', 'theme': 'default', 'ctx': 1, 'vt': '', 'referral_id': 671372, 'map': {}, 'channel_id': 14619, 'ext': {'mark': 3201, 'push_idx': 1, 'push_id': '338081', 'push_time': 1610692999}, 'network': '4g', 'uid': 10, '@timestamp': '2021-01-19T06:42:57.815Z', 'time': '2021-01-19 14:42:57', 'timestamp': '2021-01-19T06:42:57.000Z', 'method': 'GET', 'openid': 'oqhu86meMbAG3c4eQeMpp8NB35j0'}}, {'_index': 'logstash-qiyue-sound-accesslog-2021.01', '_type': '_doc', '_id': 'Txs6KHcBMuWH_P61qD-J', '_score': 5.8846154, '_source': {'tag': 701, 'os': 2, 'track': {}, 'ip': '36.112.33.135', 'admin_id': 14616, 'agent_id': 0, '@version': '1', 'page': '/index/user/recent', 'path': '/data/h5cps_heiyan/cps_access_2021-01-22_11.log', 'host': 'wx5853600fe5ce9c5b.pinduzw.net', 'theme': 'default', 'ctx': 1, 'vt': '', 'referral_id': 671353, 'map': {'type': '0', 'from': 'paid-success'}, 'env': 'production', 'channel_id': 14616, 'ext': {'mark': 3201, 'push_idx': 1, 'push_id': '338039', 'push_time': 1609213744}, 'network': '4g', 'uid': 3, '@timestamp': '2021-01-22T03:54:28.770Z', 'time': '2021-01-22 11:54:28', 'timestamp': '2021-01-22T03:54:28.000Z', 'method': 'GET', 'openid': 'opDkt59OY5CQ9KWtRXAN75SfqSVA'}}, {'_index': 'logstash-qiyue-sound-accesslog-2021.01', '_type': '_doc', '_id': 'Dhs-KHcBMuWH_P61VlbC', '_score': 5.8846154, '_source': {'tag': 701, 'os': 2, 'track': {}, 'ip': '36.112.33.135', 'admin_id': 14616, 'agent_id': 0, '@version': '1', 'page': '/index/user/recent', 'path': '/data/h5cps_heiyan/cps_access_2021-01-22_11.log', 'host': 'wx5853600fe5ce9c5b.pinduzw.net', 'theme': 'default', 'ctx': 1, 'vt': '', 'referral_id': 671353, 'map': {'type': '0', 'from': 'paid-success'}, 'env': 'production', 'channel_id': 14616, 'ext': {'mark': 3201, 'push_idx': 1, 'push_id': '338039', 'push_time': 1609213744}, 'network': '4g', 'uid': 3, '@timestamp': '2021-01-22T03:58:29.979Z', 'time': '2021-01-22 11:58:29', 'timestamp': '2021-01-22T03:58:29.000Z', 'method': 'GET', 'openid': 'opDkt59OY5CQ9KWtRXAN75SfqSVA'}}, {'_index': 'logstash-qiyue-sound-accesslog-2021.01', '_type': '_doc', '_id': 'SutjKHcBnWP96uv9Vfuj', '_score': 5.8846154, '_source': {'tag': 701, 'os': 2, 'track': {}, 'ip': '36.112.33.135', 'admin_id': 14616, 'agent_id': 0, '@version': '1', 'page': '/index/user/recent', 'path': '/data/h5cps_heiyan/cps_access_2021-01-22_12.log', 'host': 'wx5853600fe5ce9c5b.pinduzw.net', 'theme': 'default', 'ctx': 1, 'vt': '', 'referral_id': 671353, 'map': {'src': '%E5%85%AC%E4%BC%97%E5%8F%B7%E8%8F%9C%E5%8D%95%E6%A0%8F'}, 'env': 'production', 'channel_id': 14616, 'ext': {'mark': 3201, 'push_idx': 1, 'push_id': '338039', 'push_time': 1609213744}, 'network': '4g', 'uid': 3, '@timestamp': '2021-01-22T04:38:54.523Z', 'time': '2021-01-22 12:38:54', 'timestamp': '2021-01-22T04:38:54.000Z', 'method': 'GET', 'openid': 'opDkt59OY5CQ9KWtRXAN75SfqSVA'}}, {'_index': 'logstash-qiyue-sound-accesslog-2021.01', '_type': '_doc', '_id': 'ch6oKHcBMuWH_P61UTbw', '_score': 5.8846154, '_source': {'tag': 701, 'os': 2, 'track': {}, 'ip': '36.112.33.135', 'admin_id': 14616, 'agent_id': 0, '@version': '1', 'page': '/index/user/recent', 'path': '/data/h5cps_heiyan/cps_access_2021-01-22_13.log', 'host': 'wx5853600fe5ce9c5b.pinduzw.net', 'theme': 'default', 'ctx': 1, 'vt': '', 'referral_id': 671353, 'map': {'type': '0', 'from': 'paid-success'}, 'env': 'production', 'channel_id': 14616, 'ext': {'mark': 3201, 'push_idx': 1, 'push_id': '338039', 'push_time': 1609213744}, 'network': '4g', 'uid': 3, '@timestamp': '2021-01-22T05:54:15.561Z', 'time': '2021-01-22 13:54:14', 'timestamp': '2021-01-22T05:54:14.000Z', 'method': 'GET', 'openid': 'opDkt59OY5CQ9KWtRXAN75SfqSVA'}}, {'_index': 'logstash-qiyue-sound-accesslog-2021.01', '_type': '_doc', '_id': '4FaoKHcBxbLrQxJe0hX7', '_score': 5.8846154, '_source': {'tag': 701, 'os': 2, 'track': {}, 'ip': '36.112.33.135', 'admin_id': 14616, 'agent_id': 0, '@version': '1', 'page': '/index/user/recent', 'path': '/data/h5cps_heiyan/cps_access_2021-01-22_13.log', 'host': 'wx5853600fe5ce9c5b.pinduzw.net', 'theme': 'default', 'ctx': 1, 'vt': '', 'referral_id': 671353, 'map': {'type': '0', 'from': 'paid-success'}, 'env': 'production', 'channel_id': 14616, 'ext': {'mark': 3201, 'push_idx': 1, 'push_id': '338039', 'push_time': 1609213744}, 'network': '4g', 'uid': 3, '@timestamp': '2021-01-22T05:54:48.594Z', 'time': '2021-01-22 13:54:47', 'timestamp': '2021-01-22T05:54:47.000Z', 'method': 'GET', 'openid': 'opDkt59OY5CQ9KWtRXAN75SfqSVA'}}]

