# # -*- coding: utf-8 -*-
# """
# @Describe:
# @Time    : 2022/2/15 4:40 下午
# @Author  : liuhuangshan
# @File    : models.py
# """
#
#
# import re
# import subprocess
# from prometheus_client.core import GaugeMetricFamily, REGISTRY
# from prometheus_client import make_wsgi_app
# from wsgiref.simple_server import make_server
#
#
# class CustomCollector(object):
#     def add(self, params):
#         sum = 0
#         for i in params:
#             sum += int(i)
#         return sum
#
#     def collect(self):
#         output = subprocess.Popen("scontrol show nodes",
#                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
#         out_put = output.communicate()[0]
#         if out_put:
#             count = re.findall(r'CPUTot=(\d+)', out_put)
#             total_c = self.add(count)
#             yield GaugeMetricFamily('slurm_cpu_total', 'total_count', value=total_c)
#
#             used = re.findall(r'CPUAlloc=(\d+)', out_put)
#             used_cpu = self.add(used)
#             yield GaugeMetricFamily('slurm_cpu_used', 'used_count', value=used_cpu)
#
#             real_memory = re.findall(r'RealMemory=(\d+)', out_put)
#             total_memory = self.add(real_memory)
#             yield GaugeMetricFamily('slurm_memory_total', 'total_memory', value=total_memory)
#
#             alloc_memory = re.findall(r'AllocMem=(\d+)', out_put)
#             used_memory = self.add(alloc_memory)
#             yield GaugeMetricFamily('slurm_memory_used', 'used_memory', value=used_memory)
#
#
# REGISTRY.register(CustomCollector())
#
# if __name__ == '__main__':
#     coll = CustomCollector()
#     for i in coll.collect():
#         print(i)
#
#     app = make_wsgi_app()
#     httpd = make_server('', 8000, app)
#     httpd.serve_forever()
