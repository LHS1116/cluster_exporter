# -*- coding: utf-8 -*-
""" 
@Describe: 
@Time    : 2022/2/20 7:22 下午
@Author  : liuhuangshan
@File    : utils.py
"""
import re
import subprocess
import sys
import traceback
from typing import Dict, List


def format_data(data: List[str]) -> List[Dict]:
    keys = list(filter(lambda x:x.strip(), data[0].split()))
    res = []
    for val in data[1:]:
        _value = list(filter(lambda x: x.strip(), val.split()))
        assert len(_value) == len(keys)
        res.append(dict(zip(keys, _value)))
    return res


def get_squeue_data() -> List[Dict]:
    command = 'squeue'
    res = subprocess.getoutput(command).split('\n')
    data = format_data(res)
    return data


def get_sacct_data(job_id=None, username=None) -> List[Dict]:
    """{$job_id: {$column: $value,...}}"""
    columns = ['User', 'JobID', 'State', 'QOS', 'AveRSS', 'ReqCPUS', 'ReqTRES']
    fmt = ','.join(columns)
    command = f'sacct -a --format="{fmt}" {f"-j {job_id}" if job_id else ""}'  # 根据需要增加
    print(command)
    _list = []
    res = subprocess.getoutput(command).split('\n')
    res = list(filter(lambda x: x.find('ignoring it') == -1, res))
    char_count_list = list(map(lambda x: len(x), res[1].split()))  # 获取每个字段的字符下标长度范围
    assert len(char_count_list) == len(columns)
    data = res[2:]

    _res = []
    for val in data:
        flag = 0
        _list = []
        for i in char_count_list:
            _list.append(val[flag:flag + i])
            flag += i
            flag += 1  # 空格
        _list = map(lambda x: x.strip(), _list)
        tmp = dict(zip(columns, _list))
        if not username or tmp.get('User') == username:
            _res.append(tmp)
    return _res


def get_sstat_data(job_id=None):
    pass

def get_job_id_filter(job_stat: str = None) -> List[int]:
    squeue_data = get_squeue_data()
    if len(squeue_data) == 0:
        return []
    values = squeue_data
    if job_stat:
        values = filter(lambda x: x.get('ST') == job_stat, values)
    values = filter(lambda x: x, map(lambda x: x.get('JOBID'), values))
    values = list(map(lambda x: int(x), values))
    return values

def get_job_user(job_id) -> str:
    command = f'scontrol show job {job_id}'
    res = subprocess.getoutput(command)
    user_exp = r"UserId=([^\(]+)\(\d+\)"
    # print(re.compile(user_exp).search(a).groups())
    user_name = re.search(user_exp, res).group(1)
    assert len(user_name) >= 1, f'slurm job(id={job_id}) not exists'
    return user_name

def get_job_stats(job_id) -> Dict:
    command = f'sstat -a --format="JobId,Pids,AveCPU,AveRSS,AveCPUFreq,MaxRSS" -j {job_id}'
    res = subprocess.getoutput(command).split('\n')
    try:
        assert len(res) == 3
        keys = res[0].split()
        values = res[2].split()
        assert len(keys) == len(values)
        _dict = dict(zip(keys, values))
        return _dict
    except Exception as e:
        print(e)
        traceback.print_tb(sys.exc_info()[2])
        return {}


def get_cluster_gpu_stats() -> Dict:
    """eg:
    {
        'total_gpu_info': {'rtx6k': 3, 'v100': 2, 'm40': 3, 'p40': 4},
        'accessible_gpu_info': {'rtx6k': 1, 'v100': 2, 'm40': 3, 'p40': 4},
        'user_usage_info': {'user1': 1, 'user2': 1, 'user3': 3},
        'available_gpu_info': {'p40': 0, 'rtx6k': 1, 'v100': 2, 'm40': 2}
    }
    """
    try:
        command = "slurm_gpustat"
        res = subprocess.getoutput(command)
        res = res.split('----------------------')
        res = list(filter(lambda x: x.strip(), res))[1:]
        for val in res:
            data = list(filter(lambda x: x, val.split('\n')))
            print(data)
        total_gpu_info = list(filter(lambda x: x.strip(), res[0].split('\n')))[1:]
        accessible_gpu_info = list(filter(lambda x: x.strip(), res[1].split('\n')))[1:]
        user_usage_info = list(filter(lambda x: x.strip(), res[2].split('\n')))[1:]
        available_gpu_info = list(filter(lambda x: x.strip(), res[3].split('\n')))[1:]
        _dict = {'total_gpu_info': {}, 'accessible_gpu_info': {}, 'user_usage_info': {}, 'available_gpu_info': {}}

        for val in total_gpu_info:
            info = val.split()
            _dict['total_gpu_info'][info[1]] = int(info[0])
        for val in accessible_gpu_info:
            info = val.split()
            _dict['accessible_gpu_info'][info[1]] = int(info[0])
        for val in user_usage_info:
            info = list(filter(lambda x: x, val.split()))
            _dict['user_usage_info'][info[0]] = int(info[2])
        for val in available_gpu_info:
            info = val.split(': ')
            _dict['available_gpu_info'][info[0]] = int(info[1])
        return _dict
    except Exception as e:
        print(e)
        traceback.print_tb(sys.exc_info()[2])
        return {}



def get_history_job_and_user() -> Dict:
    try:
        res = get_sacct_data()

        user_dict = {}
        # nums = [str(i) for i in range(10)]
        for val in res:
            user, job_id = val.get('User'), val.get('JobID')
            assert user
            _list = user_dict.get(user, [])
            _list.append(int(job_id))
            user_dict[user] = _list
        return user_dict
    except Exception as e:
        print(e)
        traceback.print_tb(sys.exc_info()[2])
        return {}


def get_history_jobs_by_user(username) -> List[int]:
    user_dict = get_history_job_and_user()
    return user_dict.get(username, [])


def get_running_jobs_by_user(username) -> List[int]:
    squeue_data = get_squeue_data()
    if len(squeue_data) == 0:
        return []
    values = squeue_data
    values = map(lambda x: int(x.get('JOBID')), filter(lambda x: x.get('ST') == 'R' and x.get('USER') == username, values))
    return list(values)



def get_online_users() -> List[str]:
    try:
        data = get_squeue_data()
        users = set(filter(lambda x: x, map(lambda x: x.get('USER'), data)))

        return list(users)
    except Exception as e:
        print(e)
        traceback.print_tb(sys.exc_info()[2])
        return []
    pass


def get_gpu_usage_by_job(job_id) -> float:

    pass


print(get_history_job_and_user())
get_history_jobs_by_user('root')
print(get_online_users())
print(get_job_id_filter())
print(get_running_jobs_by_user('root'))
print(get_running_jobs_by_user('roott'))
print(get_sacct_data()[:5])
