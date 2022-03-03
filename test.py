# -*- coding: utf-8 -*-
""" 
@Describe: 
@Time    : 2022/1/19 5:06 下午
@Author  : liuhuangshan
@File    : utils.py
"""
from typing import List, Dict, Union

import requests
from itsdangerous import TimedSerializer

import subprocess

sec_key = "w183$sjOv&"
serializer = TimedSerializer(sec_key)

def dumps_data():
    """将data用sec_key 加密"""
    data = {'a': 111, 'b': [222, 333]}
    _data = serializer.dumps(data)
    return _data

# def loads_data


def format_data(data: str):
    lines = data.split('\n')
    res = []
    if len(lines) == 1:
        return lines
    names = lines[0].split()
    values = lines[1:]
    for v in values:
        _v = v.split()
        res.append(dict(zip(names, _v)))
    return res
    # print(res)


def get_slurm_diag():
    """生成诊断信息"""
    return ''


def test():
    url = 'http://0.0.0.0:8000/alert/create'
    data = {
        'total_gpu_info': {'rtx6k': 3, 'v100': 2, 'm40': 3, 'p40': 4},
        'accessible_gpu_info': {'rtx6k': 1, 'v100': 2, 'm40': 3, 'p40': 4},
        'user_usage_info': {'user1': 1, 'user2': 1, 'user3': 3},
        'available_gpu_info': {'p40': 0, 'rtx6k': 1, 'v100': 2, 'm40': 2}
    }
    resp = requests.post(url, json=data)
    print(resp.content)
# test()
a = []
b = [1, 2]
a.append(b)
print(a)
b = []
print(a)

# print(type(dumps_data()))