# coding=utf8
import pandas as pd
from pandas import DataFrame as df
from emtools import sql_code
from emtools import currency_means as cm
from emtools import read_database as rd
from emtools import emdate
import datetime as dt
import random
import os


class SynMonitor:

    def __init__(self, write_db, write_tab, date_col, extend='continue'):
        self.market_host = {'host': '172.16.0.248', 'user': 'root', 'pw': 'Qiyue@123'}
        self.syn_host = rd.read_db_host(
            os.path.split(os.path.realpath(__file__))[0] + '/config.yml').replace('syn_monitor', 'emtools')
        self.s_date = None
        self.write_db = write_db
        self.write_tab = write_tab
        self.date_col = date_col
        self.extend = extend
        self.refresh = None

    def step_run(self, func, process_num=16, run_num=512, interval=0.03, step=1, *args):
        if isinstance(run_num, int):
            tars = [_ for _ in range(run_num)]
        else:
            tars = run_num
        tar_date_list = self.get_date()
        for _day in tar_date_list:
            print('****** Start to run: {d} - {tab} ******'.format(d=_day, tab=self.write_tab))
            cm.thread_work(
                func, *args, self.syn_host, self.write_db, self.write_tab, _day,
                tars=tars, process_num=process_num, interval=interval, step=step
            )

    def syn_step_run(self, func, process_num=16, run_num=512, interval=0.03, step=1, *args):
        if isinstance(run_num, int):
            tars = [_ for _ in range(run_num)]
        else:
            tars = run_num
        tar_date_list = self.get_date()
        host_config = self.syn_host['shart_host']
        for _day in tar_date_list:
            print('****** Start to run: {d} - {tab} ******'.format(d=_day, tab=self.write_tab))
            cm.thread_work(
                func, *args, host_config, _day,
                tars=tars, process_num=process_num, interval=interval, step=step
            )

    def direct_run(self, func, *args):
        if self.date_col:
            tar_date_list = self.read_last_date(is_list=0)[0]
        else:
            tar_date_list = self.s_date
        func(self.market_host, self.write_db, self.write_tab, self.date_col, tar_date_list, *args)

    def get_date(self):
        tar_date_list = [0]
        if self.extend == 'list':
            tar_date_list = self.read_last_date()
        if self.extend == 'continue':
            tar_date_list = self.read_last_date(is_list=0)
        return tar_date_list

    def read_last_date(self, is_list=1, date_format='{Y}-{M}-{D}'):
        if self.s_date:
            _date = self.s_date
        else:
            conn = rd.connect_database_host(self.market_host['host'], self.market_host['user'], self.market_host['pw'])
            _date = rd.read_last_date(conn, self.write_db, self.write_tab, date_type_name=self.date_col)
            conn.close()
        if is_list:
            _date = emdate.date_list(_date, e_date=dt.datetime.now(), format_code=date_format)
            _date.sort()
        else:
            _date = [_date]
        return _date


def pick_conn_host(num, host_config):
    _steps, _ = host_config['region'].pop(0), 0
    while num >= _steps:
        _steps = host_config['region'].pop(0)
        _ += 1
    return [host_config['host'][_], host_config['user'], host_config['pw']]





