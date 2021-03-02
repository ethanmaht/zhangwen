from sshtunnel import SSHTunnelForwarder
import yaml
import pymysql
import data_job


def read_db_host(file_path):
    _file = open(file_path, 'r', encoding='utf-8')
    cont = _file.read()
    config = yaml.load(cont, Loader=yaml.FullLoader)
    return config


def format_db_name(db_name, _num):
    return "{db_name}_{_num}".format(db_name=db_name, _num=_num)


def connect_database(host, db_name):
    server = SSHTunnelForwarder(
        ('123.56.72.6', 22),
        ssh_username='root',
        ssh_password='#X@VnR4bVk!yF1LI9k7Mxq7h',
        remote_bind_address=(host, 3306)  # 远程数据库的IP和端口
    )
    server.start()
    conn = pymysql.connect(
        host='127.0.0.1', port=server.local_bind_port,
        user='cps_select', passwd='KU4CsBwrVKpmXt@4yk&LBDuI', db=db_name
    )
    print(f'****** connect success: {db_name} ******')
    return conn


class DataBaseWork:

    def __init__(self):
        self.data_list = read_db_host('config.yml')['database_host']
        self.size = {}
        self.host = ''

    def loop_all_database(self):
        for _db in self.data_list:
            data_base = _db['data_base']
            self.host = data_base['host']
            self.loop_size_table(data_base)

    def loop_size_table(self, data_base):
        if 'size' in data_base.keys():
            self.size = data_base['size']
            bases = data_base['base']
            for _db in bases.keys():
                self.loop_size_conn(_db)
        else:
            bases = data_base['base']
            for _db in bases.keys():
                self.no_size_conn(_db)

    def loop_size_conn(self, base):
        size = self.size
        _s, _e = size['start'], size['end']
        while _e >= _s:
            db_name = format_db_name(base, _s)
            conn = connect_database(self.host, db_name)
            switch_job(base, conn)
            conn.close()
            _s += 1

    def no_size_conn(self, db_name):
        conn = connect_database(self.host, db_name)
        switch_job(db_name, conn)
        conn.close()


def switch_job(db_name, conn):
    print(db_name)
    if db_name == 'cps_user':
        data_job.test_work(conn, db_name)


if __name__ == "__main__":
    # print(read_db_host('config.yml').has_key('database_host'))
    a = DataBaseWork()
    a.loop_all_database()
    # ymlymla = {'k': 'dfe', 'y': 'dfaef'}
    # format_db_name(a, 12)
