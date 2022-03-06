#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os

from flask import Flask, Response
from prometheus_client import generate_latest, CollectorRegistry, Gauge, Histogram
from utils import *

from logging.config import dictConfig
path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
log_file = os.path.join(path, "log/exporter.log")
log_dict = {
        'version': 1,
        'root': {
            # handler中的level会覆盖掉这里的level
            'level': 'DEBUG',
            'handlers': ['error_file']
        },

        'handlers': {
            'wsgi': {
                'class': 'logging.StreamHandler',
                'stream': 'ext://flask.logging.wsgi_errors_stream',
                'formatter': 'default'
            },
            "error_file": {
                "class": "logging.handlers.RotatingFileHandler",
                "maxBytes": 1024 * 1024,  # 打日志的大小，单位字节，这种写法是1M
                "backupCount": 1,
                "encoding": "utf-8",
                "level": "ERROR",
                "formatter": "default",  # 对应下面的键
                "filename": log_file  # 打日志的路径
            },
        },
        'formatters': {
            'default': {
                'format': '[%(asctime)s] %(levelname)s in [%(filename)s:%(lineno)s]: %(message)s',
            },
            'simple': {
                'format': '%(asctime)s - %(levelname)s - %(message)s'
            }
        },
    }
dictConfig(log_dict)

exporter = Flask(__name__)
registry = CollectorRegistry()

ave_gpu_usage_gauge = Gauge('ave_gpu_usage_gauge', 'show ave gpu usage by job', ['job_id', 'username'], registry=registry)
ave_cpu_usage_gauge = Gauge('ave_cpu_usage_gauge', 'show ave gpu usage by job', ['job_id', 'username'], registry=registry)

ave_mem_usage_gauge = Gauge('ave_mem_usage_gauge', 'show ave mem usage by job', ['job_id', 'username'], registry=registry)
ave_user_mem_usage = Gauge('ave_user_mem_usage_gauge', 'show ave mem usage by user', ['job_id'], registry=registry)
job_running_time = Gauge('job_running_time', 'show running_time by job', ['job_id', 'username'], registry=registry)
ave_history_user_mem_usage = Gauge('ave_history_user_mem_usage', 'show ave history mem usage by user', ['username'], registry=registry)

user_gpu_usage_histogram = Histogram('user_gpu_usage_histogram', 'show history gpu usage by user', ['username'], registry=registry)
user_mem_usage_histogram = Histogram('user_mem_usage_histogram', 'show history mem usage by user', ['username'], registry=registry)

@exporter.route('/metrics')
def get_metrics():
    running_jobs = get_job_id_filter('R')  # 获取运行中的作业信息
    # cur_users = get_online_users()
    for job_id in running_jobs:
        job_stat = get_job_stats(job_id)
        job_user = get_job_user(job_id)
        if job_stat:
            ave_job_mem = job_stat.get('AveCPU', 0)
            # ave_gpu = get_gpu_usage_by_job(job_id)
            ave_mem_usage_gauge.labels(job_id, job_user).set(ave_job_mem)
            # ave_gpu_usage_gauge.labels(job_id, job_user).set(ave_gpu)
    # for user in cur_users:
    #     history_ave_mem = list(map(lambda x: int(x.get('AveRSS', '0K')[:-1]), get_sacct_data(user)))
    #     ave_value = sum(history_ave_mem) / len(history_ave_mem)
    #     ave_history_user_mem_usage.labels(user).set(ave_value)
    # 上传数据
    upload_data()
    # 检查是否有需要关闭的任务


    return Response(generate_latest(registry), mimetype='text/plain')



if __name__ == '__main__':
    exporter.run(host='0.0.0.0', port=5050)
