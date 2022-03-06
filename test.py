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
    res = """ACCOUNT|TRES_PER_NODE|MIN_CPUS|MIN_TMP_DISK|END_TIME|FEATURES|GROUP|OVER_SUBSCRIBE|JOBID|NAME|COMMENT|TIME_LIMIT|MIN_MEMORY|REQ_NODES|COMMAND|PRIORITY|QOS|REASON||ST|USER|RESERVATION|WCKEY|EXC_NODES|NICE|S:C:T|JOBID|EXEC_HOST|CPUS|NODES|DEPENDENCY|ARRAY_JOB_ID|GROUP|SOCKETS_PER_NODE|CORES_PER_SOCKET|THREADS_PER_CORE|ARRAY_TASK_ID|TIME_LEFT|TIME|NODELIST|CONTIGUOUS|PARTITION|PRIORITY|NODELIST(REASON)|START_TIME|STATE|UID|SUBMIT_TIME|LICENSES|CORE_SPEC|SCHEDNODES|WORK_DIR
(null)|N/A|1|0|NONE|(null)|root|OK|88|testslurm.sh|(null)|UNLIMITED|1G||/home/bytedance/testslurm.sh|0.99998473934829|(null)|None||R|root|(null)|(null)||0|*:*:*|88|lhs-01|1|1||88|0|*|*|*|N/A|UNLIMITED|0:23|lhs-01|0|debug|4294901751|lhs-01|2022-03-06T02:24:14|RUNNING|0|2022-03-06T02:24:14|(null)|N/A|(null)|/home/bytedance"""
    res = res.split('\n')
    a0 = res[0].split('|')
    a1 = res[1].split('|')
    dt = dict(zip(a0, a1))
    for d in dt:
        print(f"{d} : {dt[d]}")
def test1():
    res = """JobID  MaxVMSize  MaxVMSizeNode  MaxVMSizeTask  AveVMSize     MaxRSS MaxRSSNode MaxRSSTask     AveRSS MaxPages MaxPagesNode   MaxPagesTask   AvePages     MinCPU MinCPUNode MinCPUTask     AveCPU   NTasks AveCPUFreq ReqCPUFreqMin ReqCPUFreqMax ReqCPUFreqGov ConsumedEnergy  MaxDiskRead MaxDiskReadNode MaxDiskReadTask  AveDiskRead MaxDiskWrite MaxDiskWriteNode MaxDiskWriteTask AveDiskWrite TRESUsageInAve TRESUsageInMax TRESUsageInMaxNode TRESUsageInMaxTask TRESUsageInMin TRESUsageInMinNode TRESUsageInMinTask TRESUsageInTot TRESUsageOutAve TRESUsageOutMax TRESUsageOutMaxNode TRESUsageOutMaxTask TRESUsageOutMin TRESUsageOutMinNode TRESUsageOutMinTask TRESUsageOutTot 
------------ ---------- -------------- -------------- ---------- ---------- ---------- ---------- ---------- -------- ------------ -------------- ---------- ---------- ---------- ---------- ---------- -------- ---------- ------------- ------------- ------------- -------------- ------------ --------------- --------------- ------------ ------------ ---------------- ---------------- ------------ -------------- -------------- ------------------ ------------------ -------------- ------------------ ------------------ -------------- --------------- --------------- ------------------- ------------------- --------------- ------------------- ------------------- --------------- 
82.batch         78972K         lhs-01              0     10700K      1188K     lhs-01          0      1180K        1       lhs-01              0          1  00:00.000     lhs-01          0  00:00.000        1      2.81M             0             0             0              0         8909          lhs-01               0         8909            0           lhs-01                0            0 cpu=00:00:00,+ cpu=00:00:00,+ cpu=lhs-01,energy+ cpu=00:00:00,fs/d+ cpu=00:00:00,+ cpu=lhs-01,energy+ cpu=00:00:00,fs/d+ cpu=00:00:00,+ energy=0,fs/di+ energy=0,fs/di+ energy=lhs-01,fs/d+           fs/disk=0 energy=0,fs/di+ energy=lhs-01,fs/d+           fs/disk=0 energy=0,fs/di+"""

    res = res.split('\n')


    res = list(filter(lambda x: x.find('ignoring it') == -1, res))
    columns = res[0].split()
    _list = []
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
        print(tmp)
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
get_slurm_diag()

# print(type(dumps_data()))