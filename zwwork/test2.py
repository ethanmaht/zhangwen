import requests
import json
from pandas import DataFrame as df
import os
from emtools import read_database as rd

a = ['宜春市宜春大道698号绿野天城1栋二单元201号', '宜春市袁州区西村镇老水泥厂', '宜丰县青草坑', '樟树市人民医院。', '个体街袁山西路299号', '明月北路', '宜春市宜阳小区18栋1单元902', '万载县株潭镇亭下村', '丰城市第二中学。', '丰城市解放北路公安局110指挥中心']
b = ['袁州区', '樟树市', '丰城市', '靖安县', '奉新县', '高安市', '上高县', '宜丰县', '铜鼓县', '万载县']
d = {}

for _a in a:
    x = True
    for _b in b:
        if _b in _a:
            # c.append(_b)
            d.update({_a: _b})
            x = False
            break
    if x:
        d.update({_a: ''})


print(d)
