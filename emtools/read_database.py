from sshtunnel import SSHTunnelForwarder
import yaml
import pymysql


def read_db_host(file_path):
    _file = open(file_path, 'r', encoding='utf-8')
    cont = _file.read()
    config = yaml.load(cont)
    return config


def format_db_name(db_name, _num):
    for _key in db_name.keys():
        db_name[_key] = "{db_name}_{_num}".format(db_name=db_name[_key], _num=_num)
    print(db_name)
    return db_name


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
    return conn


read_db_host('config.yml')

a = {'k': 'dfe', 'y': 'dfaef'}
format_db_name(a, 12)
