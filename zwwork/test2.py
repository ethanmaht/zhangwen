import requests
import lxml


jd = """
https://search.jd.com/Search?keyword=%E9%A5%AE%E6%96%99&qrst=1
&wq=%E9%A5%AE%E6%96%99&stock=1&pvid=317be2ae751e428db0aeff239a7bd6af&page=3&s=56&click=0
"""

tt = """
https://zhuanlan.zhihu.com/p/29436838
"""
headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
    'Connection': 'keep-alive',
    'Referer': 'http://www.baidu.com/'
}

a = requests.get(tt, headers)
# b = lxml.etree.HTML(a.text)
print(a.text)
