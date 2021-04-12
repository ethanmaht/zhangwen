import time
from pandas import DataFrame as df
from algorithm import retained
from es_worker import ec_market
from emtools import emdate
from es_worker import es_tool


es_tool.run_read_ex_loop(
    sub_days=1, size=10000, write_conn_fig='datamarket', write_db='sound', tab='es_log',
    index='logstash-qiyue-accesslog*'
)  # 七悦的es同步_测试

