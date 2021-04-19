import pandas as pd
from sqlalchemy import create_engine


class Company:

    def __init__(self):
        self.config = {
            'glory': "mysql+pymysql://dianzhong:Heiyan@123@47.98.63.24:13306/glory_sum",
            'kyycps': "mysql+pymysql://dianzhong:Heiyan@123@47.98.63.24:13306/kyycps",
            'userdata': "mysql+pymysql://dianzhong:Heiyan@123@47.98.63.24:13306/glory_userdata"
        }

    def conn_db(self, db_name):
        config = self.config[db_name]
        conn = create_engine(config)
        return conn

    def top_up(self):
        conn_a = self.conn_db('glory')
        pass

    def channel_code_group(self):
        pass

    def day_group(self):
        pass

    def book_consume(self):
        print()
        pass

    def one_book_consume(self):
        pass

    def consume(self):
        pass

    def recharge_num(self):
        pass

    def orders_top_up(self):
        pass

    def costing(self):
        pass