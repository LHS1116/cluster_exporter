# -*- coding: utf-8 -*-
""" 
@Describe: 
@Time    : 2022/2/20 7:22 下午
@Author  : liuhuangshan
@File    : utils.py
"""
import datetime
import random
import re
import subprocess
import sys
import traceback
from typing import Dict, List
import time
from typing import List, Dict, Union
import threading
import requests
from itsdangerous import TimedSerializer, TimestampSigner
from models import *
sec_key = "w183$sjOv&"
serializer = TimedSerializer(sec_key)
signer = TimestampSigner(sec_key)
web_domain = 'http://192.168.137.12:8000/api'


def transfer_time(time_str):
    if not time_str:
        return None
    return datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")


def get_seconds(time_str):
    """h:m:s -> seconds"""
    lst = time_str.split(':')
    seconds = int(lst[0]) * 3600 + int(lst[1]) * 60 + int(lst[2])
    return seconds


def cal_timedelta(start: datetime, end: datetime) -> str:
    if not start or not end:
        return ''
    seconds = int((end - start).total_seconds())
    days = int(seconds / (3600 * 24))
    seconds %= (3600 * 24)
    hours = int(seconds / 3600)
    seconds %= 3600
    minutes = int(seconds / 60)
    seconds %= 60
    seconds = round(seconds, 2)
    res = ''
    if days:
        res += f'{days} d '
    if hours:
        res += f'{hours} h '
    if minutes:
        res += f'{minutes} m '
    res += f'{seconds} s'
    return res

def generate_token(value: str):
    try:
        t = signer.sign(value)
        return t
    except Exception as e:
        print(e)
        return ''

def format_data(data: List[str]) -> List[Dict]:
    keys = list(filter(lambda x: x.strip(), data[0].split()))
    res = []
    for val in data[1:]:
        _value = list(filter(lambda x: x.strip(), val.split()))
        assert len(_value) == len(keys)
        res.append(dict(zip(keys, _value)))
    return res

def format_node_name(name: List):
    _nodes = []
    for node in name:
        if node.find('[') == -1:
            _nodes.append(node)
        else:
            _tmp = node.replace(']', '')
            _tmp = _tmp.split('[')
            _labels = _tmp[1].split('-')
            for _label in _labels:
                _nodes.append(_tmp[0] + _label)
    return _nodes

def get_squeue_data(all=False) -> List[Dict]:
    if not all:
        command = 'squeue'
        res = subprocess.getoutput(command).split('\n')
        data = format_data(res)
    else:
        command = 'squeue -o %all'
        res = subprocess.getoutput(command).split('\n')
        keys = res[0].split('|')
        data = []
        for val in res[1:]:
            _vals = val.split('|')
            assert len(_vals) == len(keys)
            data.append(dict(zip(keys, _vals)))
    return data


def get_sinfo_data(return_dict=False) -> List[Union[SlurmPartition, Dict]]:
    """获取队列信息"""
    res = """PARTITION$AVAIL$NODES(A/I)$MAX_CPUS_PER_NODE$CPUS$CPUS(A/I/O/T)$NODES(A/I/O/T)$GRES$NODELIST$S:C:T
debug*$up$0/2$UNLIMITED$2$0/4/0/4$0/2/0/2$(null)$lhs-[01-02]$2:1:1"""
    res = res.split('\n')
    command = "sinfo -o %P$%a$%A$%B$%c$%C$%F$%G$%N$%z"
    """A/I/O/T = allocated/idle/other/total"""
    """sockets、cores、threads（S:C:T）"""
    # res = subprocess.getoutput(command).split('\n')

    column_names = res[0].split('$')
    data = []
    for val in res[1:]:
        part = SlurmPartition()
        part_val = val.split('$')
        assert len(column_names) == len(part_val)
        info = dict(zip(column_names, part_val))
        part_name = info.get('PARTITION')
        if part_name and part_name[-1] == '*':
            part.default = 1
            part_name = part_name[:-1]
        cpu_aiot = info['CPUS(A/I/O/T)'].split('/')
        node_aiot = info['NODES(A/I/O/T)'].split('/')
        nodes = info['NODELIST'].split(',')
        _nodes = format_node_name(nodes)

        part.name = part_name
        part.state = info['AVAIL']
        part.cpu_total = cpu_aiot[3]
        part.cpu_alloc = cpu_aiot[0]
        part.nodes = _nodes

        #  TODO:从GRES获取GPU信息
        part.gpu_alloc = 0
        part.gpu_total = 0

        data.append(part.__dict__ if return_dict else part)
        # print(part.__dict__)
    return data


def get_sacct_data(start=None, end=None, job_id=None) -> List[Union[SlurmJob, Dict]]:
    """{$job_id: {$column: $value,...}}"""
    columns = ['User', 'JobID', 'Partition', 'JobName', 'State', 'AllocTRES', 'AllocGRES', 'AllocCPUS', 'QOS', 'AveCPU','ReqCPUS', 'CPUTime', 'TotalCPU', 'UserCPU', 'ReqTRES', 'Submit', 'Start', 'End']
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
        _res.append(tmp)
    return _res


def get_sstat_data(job_id=None) -> Union[Dict, List[Dict]]:
    columns = ['User', 'JobID', 'State', 'AveCPU', 'AveRSS', 'ReqCPUS', 'ReqTRES', 'Nodelist']
    fmt = ','.join(columns)
    command = f'sstat -a --format="{fmt}" {f"-j {job_id}" if job_id else ""}'  # 根据需要增加

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
        _res.append(tmp)
    return _res
    # for


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
        res = {
            'total_gpu_info': {'rtx6k': 3, 'v100': 2, 'm40': 3, 'p40': 4},
            'accessible_gpu_info': {'rtx6k': 1, 'v100': 2, 'm40': 3, 'p40': 4},
            'user_usage_info': {'user1': 1, 'user2': 1, 'user3': 3},
            'available_gpu_info': {'p40': 0, 'rtx6k': 1, 'v100': 2, 'm40': 2}
        }
        return res
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


def get_running_jobs(username=None) -> List[int]:
    squeue_data = get_squeue_data()
    if len(squeue_data) == 0:
        return []
    values = squeue_data
    if username:
        values = map(lambda x: int(x.get('JOBID')),
                     filter(lambda x: x.get('ST') == 'R' and x.get('USER') == username, values))
    else:
        values = map(lambda x: int(x.get('JOBID')), filter(lambda x: x.get('ST') == 'R', values))
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


def get_nodes() -> Dict:
    command = "scontrol show nodes"
    res = subprocess.getoutput(command).split('\n')
    res = filter(lambda x: x and x.find('OS=') == -1, res)
    data = []  # node_name: node
    _node = []
    for val in res:
        if val[:8] == 'NodeName' and _node:
            data.append(_node)
            _node = []

        _node.extend(val.split())
    if _node:
        data.append(_node)

    data = [map(lambda x: x.split('='), node) for node in data]
    data = [{x[0]: x[1] for x in node} for node in data]
    data = {node['NodeName']: node for node in data}
    return data


def get_slurm_jobs(return_dict = False) -> List[SlurmJob]:
    squeue_data = get_squeue_data(True)
    res = []
    for data in squeue_data:
        _job = SlurmJob()
        job_id = data.get('JOBID')
        job_stat = get_sstat_data(job_id)
        ave_cpu = get_seconds(job_stat.get('AveCPU', '00:00:00'))  # %h:%m:%s
        _start_time = transfer_time(job_stat.get('Start'))
        _end_time = transfer_time(job_stat.get('End'))
        running_time = cal_timedelta(_start_time, _end_time)
        _job.job_id = job_id
        _job.submit_time = str(transfer_time(job_stat.get('Submit')) or '')
        _job.start_time = str(_start_time or '')
        _job.end_time = str(_end_time or '')
        _job.user = job_stat.get('User')
        _job.state = job_stat.get('State')
        _job.ave_cpu_time = ave_cpu
        _job.running_time = running_time
        _job.node = format_node_name(data['NODELIST'].split(','))
        _job.partition = data['PARTITION']
        _job.cpu_alloc = data['CPUS']
        # TODO: 获取数据
        # _job.cpu_utilization = job_stat.get('CPUUtilization')


        _job.gpu_alloc = random.Random().randint(1, 10)

        _job.gpu_used = random.Random().randint(1, _job.gpu_alloc)
        # _job.cpu_used = random.Random().randint(1, _job.cpu_alloc)
        # _job.cpu_utilization = _job.cpu_used / _job.cpu_alloc
        _job.gpu_utilization = _job.gpu_used / _job.gpu_alloc
        res.append(_job.__dict__ if return_dict else _job)
    return res

def do_upload_data():
    """上传集群信息到web服务器"""
    now = datetime.now()

    current_user = get_online_users()
    sacct_info = get_sacct_data()
    nodes = get_nodes()
    gpu_stat = get_cluster_gpu_stats()
    # node_gpus = get_gpu_usage_by_node()
    # SlurmModel格式
    slurm_jobs = get_slurm_jobs(return_dict=True)
    partitions = get_sinfo_data(return_dict=True)
    data = {
        'current_user': current_user,
        'jobs': slurm_jobs,
        'all_jobs': sacct_info,
        'nodes': nodes,
        'gpu_stat': gpu_stat,
        # 'node_gpu': node_gpus,
        f'partitions_{now.strftime("%Y-%m-%d-%h")}': partitions
    }
    token = generate_token('cluster_exporter')
    header = {
        'Token': token
    }
    url = f'{web_domain}/data/upload'
    r = requests.post(url, json=data, headers=header)

    print(r.content)



def upload_data():
    threading.Thread(target=do_upload_data()).start()


def do_canceled_job(job_id: List):
    """根据id取消任务"""
    for _id in job_id:
        command = f'scancel {job_id}'
        subprocess.getoutput(command)


def update_canceled_data(job_ids: list):
    """更新web端需要取消的任务数据"""
    data = {
        'job_id': job_ids
    }
    token = generate_token('cluster_exporter')
    header = {
        'Token': token
    }
    url = f'{web_domain}/alert/callback'
    r = requests.post(url, json=data, headers=header)

    print(r.content)


def do_check_canceled_job():
    """检查是否有需要取消的任务"""
    url = f'{web_domain}/alert/check'
    r = requests.get(url)
    if r.status_code == 200:
        js = r.json()
        job_id = js['data']
        assert isinstance(job_id, list)
        do_canceled_job(job_id)
        running_jobs = get_running_jobs()
        success_canceled = []
        for jid in job_id:
            if jid not in running_jobs:
                success_canceled.append(jid)
        update_canceled_data(success_canceled)


def check_need_canceled_job():
    threading.Thread(target=do_check_canceled_job()).start()


# get_sinfo_data()