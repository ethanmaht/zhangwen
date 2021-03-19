import time
import requests
import hashlib
from pandas import DataFrame as df
import json


class RequestsData:
    def __init__(self):
        self.appKey = 'EKSTRXINLKALRCRG'
        self.appSecret = 'EKSTRXINLKALRCRG'
        self.tenantSign = 'daixiaoer'
        self.timestamp = str(round(time.time() * 1000))
        self.version = 'v1'
        self.upwd = '{tSign}appKey={aKey}&appSecret={apps}&tenantSign={tts}&version={vs}&timestamp={ts}'.format(
            tSign=self.tenantSign, aKey=self.appKey, apps=self.appSecret,
            tts=self.tenantSign, vs=self.version, ts=self.timestamp
        )

    def make_header(self):
        m = hashlib.sha256()
        m.update(self.upwd.encode("UTF-8"))
        signature = m.hexdigest()
        header = {
            'appKey': self.appKey,
            'appSecret': self.appSecret,
            'tenantSign': self.tenantSign,
            'timestamp': self.timestamp,
            'signature': signature
        }
        return header

    def get_customer_data(self, pagesize, page_num, *args, **kwargs):
        cust_url = 'https://openapi.tanyibot.com/apiOpen/v1/customerPersonInfo/customerList'
        header = self.make_header()
        _poll = []
        for _ in range(page_num):
            cust_data = {'pageSize': pagesize, 'pageNum': _}
            _date = json.dumps(cust_data)
            cust = requests.get(cust_url, data=cust_data, headers=header)
            one_page = cust.json()
            print(one_page, len(one_page))
            _poll += one_page['data']['content']
            print(len(_poll))
        return df(_poll)


if __name__ == '__main__':
    run = RequestsData()
    run.get_customer_data(10, 1)
