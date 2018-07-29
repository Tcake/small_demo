#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
调用opends接口，将数据上传到BDP
"""
import requests
from log import info
import urllib
import json
from config import config

URL = config.get('OPENDS', 'URL')
TOKEN = config.get('OPENDS', 'TOKEN')
DS_NAME = 'zlt_test'
DS_ID = ''  # 数据库id
TB_JSZ_ID = config.get('TB_JSZ', 'tb_id')  # 驾驶证表id
TB_XSZ_ID = config.get('TB_XSZ', 'tb_id')  # 行驶证表id
TB_THR_ID = config.get('TB_THR', 'tb_id')  # 同户人表id

TB_JSZ = {"remark": "",   # 表格结构数据 驾驶证
            "name": "tb_jsz",
            "title": "driving_licence",
            "ds_id": '',
            "dereplication": 1,
            "uniq_key": ["r_id", "w_id"],
            "schema": [{"remark": "", "type": "number", "name": "r_id", "title": "默认主键"},
                       {"remark": "", "type": "number", "name": "w_id", "title": "默认次主键"},
                       {"remark": "", "type": "string", "name": "zjhm", "title": "身份证号"},
                       {"remark": "", "type": "string", "name": "xm", "title": "姓名"},
                       {"remark": "", "type": "string", "name": "lxdh", "title": "联系电话"},
                       {"remark": "", "type": "string", "name": "zjcx", "title": "准驾车型"},
                       {"remark": "", "type": "string", "name": "sjhm", "title": "手机号码"},
                       {"remark": "", "type": "string", "name": "cclzrq", "title": "初次领证日期"},
                       {"remark": "", "type": "string", "name": "syrq", "title": "审验日期"},
                       {"remark": "", "type": "string", "name": "yxq", "title": "有效期"},
                       {"remark": "", "type": "string", "name": "zt", "title": "状态"},
                       {"remark": "", "type": "string", "name": "lxzs", "title": "联系住所"},
                       {"remark": "", "type": "string", "name": "djzs", "title": "登记住所"}]}
TB_XSZ = {"remark": "",   # 表格结构数据 行驶证
            "name": "tb_xsz",
            "title": "travel_license",
            "ds_id": '',
            "dereplication": 1,
            "uniq_key": ["r_id", "w_id"],
            "schema": [{"remark": "", "type": "number", "name": "r_id", "title": "默认主键"},
                       {"remark": "", "type": "number", "name": "w_id", "title": "默认次主键"},
                       {"remark": "", "type": "string", "name": "zjhm", "title": "身份证号"},
                       {"remark": "", "type": "string", "name": "hphm", "title": "号牌号码"},
                       {"remark": "", "type": "string", "name": "hpzl", "title": "号牌种类"},
                       {"remark": "", "type": "string", "name": "fdjh", "title": "发动机号"},
                       {"remark": "", "type": "string", "name": "cpxh", "title": "厂牌型号"},
                       {"remark": "", "type": "string", "name": "csys", "title": "车身颜色"},
                       {"remark": "", "type": "string", "name": "zt", "title": "状态"},
                       {"remark": "", "type": "string", "name": "syr", "title": "机动车所有人姓名"},
                       {"remark": "", "type": "string", "name": "lxdh", "title": "联系电话"},
                       {"remark": "", "type": "string", "name": "cllx", "title": "车辆类型"},
                       {"remark": "", "type": "string", "name": "sjhm", "title": "手机号码"},
                       {"remark": "", "type": "string", "name": "zzxx", "title": "住址信息"},
                       {"remark": "", "type": "string", "name": "zsxx", "title": "住所信息"},
                       {"remark": "", "type": "string", "name": "yxq", "title": "有效期"},
                       {"remark": "", "type": "string", "name": "clsbdm", "title": "车辆识别代码"},
                       {"remark": "", "type": "string", "name": "xzqh", "title": "行政区划"}]}
TB_THR = {"remark": "",   # 表格结构数据 同户人
            "name": "tb_thr",
            "title": "together_man",
            "ds_id": '',
            "dereplication": 1,
            "uniq_key": ["r_id", "w_id"],
            "schema": [{"remark": "", "type": "number", "name": "r_id", "title": "默认主键"},
                       {"remark": "", "type": "number", "name": "w_id", "title": "默认次主键"},
                       {"remark": "", "type": "string", "name": "zjhm", "title": "身份证号"},
                       {"remark": "", "type": "string", "name": "XM", "title": "姓名"},
                       {"remark": "", "type": "string", "name": "GMSFHM", "title": "证件号码"},
                       {"remark": "", "type": "string", "name": "XB", "title": "性别"},
                       {"remark": "", "type": "string", "name": "MZ", "title": "民族"},
                       {"remark": "", "type": "string", "name": "YHZGXMC", "title": "与户主关系"},
                       {"remark": "", "type": "string", "name": "SG", "title": "身高"},
                       {"remark": "", "type": "string", "name": "ZJXY", "title": "宗教信仰"},
                       {"remark": "", "type": "string", "name": "WHCD", "title": "文化程度"},
                       {"remark": "", "type": "string", "name": "CSRQ", "title": "出生日期"}]}

JSZ_FIELDS = ["r_id", "w_id", "zjhm", "xm", "lxdh", "zjcx", "sjhm", "cclzrq", "syrq", "yxq", "zt", "lxzs", "djzs"]
XSZ_FIELDS = ["r_id", "w_id", "zjhm", "hphm", "hpzl", "fdjh", "cpxh", "csys", "zt", "syr", "lxdh", "cllx",
              "sjhm", "zzxx", "zsxx", "yxq", "clsbdm", "xzqh"]
THR_FIELDS = ["r_id", "w_id", "zjhm", "XM", "GMSFHM", "XB", "MZ", "YHZGXMC", "SG", "ZJXY", "WHCD", "CSRQ"]


# 提交数据
def commit(tb_id):
    url = "{}/api/tb/commit?access_token={}&tb_id={}".format(URL, TOKEN, tb_id)
    result = json.loads(requests.get(url).content)
    if not result['status'] == '0':
        info().error('commit数据失败:{}'.format(result['errstr']))
        raise ValueError


# 插入数据
def insert_data(tb_id, fields, data):
    fiels_quote = urllib.quote(json.dumps(fields))
    url = "{}/api/data/insert?access_token={}&tb_id={}&fields={}".format(URL, TOKEN, tb_id, fiels_quote)
    result = json.loads(requests.post(url, json.dumps(data)).content)
    if not result['status'] == '0':
        info().error('插入数据失败:{}'.format(result['errstr']))
        raise ValueError


def jsz_insert(data):
    insert_data(TB_JSZ_ID, JSZ_FIELDS, data)
    info().info('插入驾驶证数据库数据成功')
    return 'success'


def xsz_insert(data):
    insert_data(TB_XSZ_ID, XSZ_FIELDS, data)
    info().info('插入行驶证数据库数据成功')
    return 'success'


def thr_insert(data):
    insert_data(TB_THR_ID, THR_FIELDS, data)
    info().info('插入同户人数据库数据成功')
    return 'success'


# 创建数据库
def create_ds(name=DS_NAME):
    global DS_ID
    url = "{}/api/ds/create?access_token={}&name={}".format(URL, TOKEN, name)
    result = json.loads(requests.get(url).content)
    if not result['status'] == '0':
        info().error('创建数据库失败:{}'.format(result['errstr']))
        raise ValueError
    DS_ID = str(result['result']['ds_id'])
    info().info('创建成功，数据库id:'+DS_ID)


# 创建表
def create_tb(data):
    url = "{}/api/tb/create?access_token={}".format(URL, TOKEN)
    data.update({'ds_id': DS_ID})
    result = json.loads(requests.post(url, json.dumps(data)).content)
    if not result['status'] == '0':
        info().error('创建工作表失败:{}'.format(result['errstr']))
        raise ValueError
    tb_id = str(result['result']['tb_id'])
    commit(tb_id)
    return tb_id


def create_jsz():
    global TB_JSZ_ID
    TB_JSZ_ID = create_tb(TB_JSZ)
    info().info('创建成功，驾驶证表id：'+TB_JSZ_ID)


def create_xsz():
    global TB_XSZ_ID
    TB_XSZ_ID = create_tb(TB_XSZ)
    info().info('创建成功，行驶证表id：'+TB_XSZ_ID)


def create_thr():
    global TB_THR_ID
    TB_THR_ID = create_tb(TB_THR)
    info().info('创建成功，同户人表id：'+TB_THR_ID)


# 初始化数据库和数据表
def init():
    create_ds()
    create_jsz()
    create_xsz()
    create_thr()


if __name__ == '__main__':
    # init()
    commit(TB_THR_ID)
