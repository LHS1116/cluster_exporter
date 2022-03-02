#!/usr/bin/env python
# -*- coding:utf-8 -*-
from flask import Flask, Response
from prometheus_client import generate_latest, CollectorRegistry, Gauge, Histogram
from utils import *

exporter = Flask(__name__)
registry = CollectorRegistry()

ave_gpu_usage_gauge = Gauge('ave_gpu_usage_gauge', 'show ave gpu usage by job', ['job_id', 'username'], registry=registry)
ave_mem_usage_gauge = Gauge('ave_mem_usage_gauge', 'show ave mem usage by job', ['job_id', 'username'], registry=registry)
ave_user_mem_usage = Gauge('ave_user_mem_usage_gauge', 'show ave mem usage by user', ['job_id'], registry=registry)
job_running_time = Gauge('job_running_time', 'show running_time by job', ['job_id', 'username'], registry=registry)
ave_history_user_mem_usage = Gauge('ave_history_user_mem_usage', 'show ave history mem usage by user', ['username'], registry=registry)

user_gpu_usage_histogram = Histogram('user_gpu_usage_histogram', 'show history gpu usage by user', ['username'], registry=registry)
user_mem_usage_histogram = Histogram('user_mem_usage_histogram', 'show history mem usage by user', ['username'], registry=registry)

@exporter.route('/metrics')
def get_metrics():
    running_jobs = get_job_id_filter('R')  # 获取运行中的作业信息
    cur_users = get_online_users()
    for job_id in running_jobs:
        job_stat = get_job_stats(job_id)
        job_user = get_job_user(job_id)
        if job_stat:
            ave_job_mem = job_stat.get('AveRSS', 0)
            ave_gpu = get_gpu_usage_by_job(job_id)
            ave_mem_usage_gauge.labels(job_id, job_user).set(ave_job_mem)
            ave_gpu_usage_gauge.labels(job_id, job_user).set(ave_gpu)
    for user in cur_users:
        history_ave_mem = list(map(lambda x: int(x.get('AveRSS', '0K')[:-1]), get_sacct_data(user)))
        ave_value = sum(history_ave_mem) / len(history_ave_mem)
        ave_history_user_mem_usage.labels(user).set(ave_value)
    return Response(generate_latest(registry), mimetype='text/plain')


if __name__ == '__main__':
    exporter.run(host='0.0.0.0', port=5050)
