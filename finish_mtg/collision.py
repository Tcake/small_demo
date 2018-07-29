#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
将主数据库得到的数据和api接口调用来的数据进行拼接
"""
from log import info

JSZ_FIELDS = ["zjhm", "xm", "lxdh", "zjcx", "sjhm", "cclzrq", "syrq", "yxq", "zt", "lxzs", "djzs"]
XSZ_FIELDS = ["zjhm", "hphm", "hpzl", "fdjh", "cpxh", "csys", "zt", "syr", "lxdh", "cllx",
              "sjhm", "zzxx", "zsxx", "yxq", "clsbdm", "xzqh"]
THR_FIELDS = ["zjhm", "XM", "GMSFHM", "XB", "MZ", "YHZGXMC", "SG", "ZJXY", "WHCD", "CSRQ"]


def collision(data, fields, now_count):
    if not isinstance(data, list):
        info().error('碰撞输入类型不正确'+'123')
        raise TypeError
    result = []
    w_id = 1
    for _data in data:
        _result = [now_count, w_id]
        for field in fields:
            _result.append(_data.get(field, ''))
        result.append(_result)
        w_id += 1
    return result
# type result:
# [[1, 1, '', '', '****', '', '', '', '', '', '', '', ''], [1, 2, '', '', '', '', '', '', '', '', '****', '', '']]


def jsz_collision(data, now_count):
    return collision(data, JSZ_FIELDS, now_count)


def xsz_collision(data, now_count):
    return collision(data, XSZ_FIELDS, now_count)


def thr_collision(data, now_count):
    return collision(data, THR_FIELDS, now_count)


if __name__ == '__main__':
    a = [{"jtzz": "*******", "cpxh": "****", "lxdh": "****", "hpzl": "****"}, {"hphm": "****", "hpzl": "****", "zt": "****", "syr": "****"}]
    # b = {"hphm": "****", "hpzl": "****", "zt": "****", "syr": "****"}
    print jsz_collision(a, 1)
