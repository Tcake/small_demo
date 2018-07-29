#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
访问用于碰撞的数据表
"""
__all__ = ['jsz_reader', 'xsz_reader', 'thr_reader']

from threading import Thread
import requests
import json
from Queue import Full

from log import info
from master_excel import master_excel
from config import config

URL_DRIVING = config.get('API', 'URL_DRIVING')
UTL_FAMILY = config.get('API', 'UTL_FAMILY')
APPKEY = config.get('API', 'APPKEY')

ETL_TYPE = 'etltype'
APP_TYPE = 'apptype'

TYPE_JSZ = 'jg.jsz'  # 驾驶证
TYPE_XSZ = 'jg.xsz'  # 行驶证
TYPE_THR = 'rygx_th.zjhm'  # 同户人
LOGUSER = ''

READ_COUNT = [0, ]


def slave(url, type_key, etl_type, zjhm):
    data = {'data': {'appkey': APPKEY, type_key: etl_type, 'loguser': LOGUSER, 'zjhm': zjhm}}
    payload = json.dumps(data)
    result = requests.post(url, data=payload).content
    result = json.loads(result)
    if result['state'] == 'NODATA':  # 修改成no data
        info().info('该身份证没有数据：{}'.format(zjhm))
        return None
    elif not result['state'] == 'OK':
        info().info('失败的身份证号：{}'.format(zjhm))
        info().error(str(result))
        return None
    return result['data']
#  return type: {"num":1,"state":"OK","data":[{"jtzz":"*******","cpxh":"****","lxdh":"****"}]}


def driving_licence(zjhm):
    return slave(URL_DRIVING, ETL_TYPE, TYPE_JSZ, zjhm)


def travel_licence(zjhm):
    return slave(URL_DRIVING, ETL_TYPE, TYPE_XSZ, zjhm)


def together_man(zjhm):
    return slave(UTL_FAMILY, APP_TYPE, TYPE_THR, zjhm)


class Reader(Thread):
    """
    用来多线程访问api
    """
    def __init__(self, task_name, outqueue, name):
        super(Reader, self).__init__()
        self.outqueue = outqueue
        self.task_name = task_name
        self.name = name
        self.read_count = READ_COUNT

    def run(self):
        while True:
            perid_list = master_excel()
            self.read_count[0] += 100
            info().info('Reader：{}读取次数：{}'.format(self.name, self.read_count[0]))
            if not perid_list:
                info().info('身份证数据全部读取完成')
                break
            for perid in perid_list:
                data = self.task_name(zjhm=perid)
                while data:
                    for _data in data:
                        _data.update({'zjhm': perid})
                    try:
                        self.outqueue.put(data, timeout=5)
                        info().info('Reader{}, write {}, qsize: {}'.format(self.name, data, self.outqueue.qsize()))
                        break
                    except Full:
                        info().info('Reader{}:queue full'.format(self.name))


def jsz_reader(outqueue, name):
    return Reader(driving_licence, outqueue, name)


def xsz_reader(outqueue, name):
    return Reader(travel_licence, outqueue, name)


def thr_reader(outqueue, name):
    return Reader(together_man, outqueue, name)


