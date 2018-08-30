#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
import json

URL = 'https://open.bdp.cn'
TB_JSZ_ID = 'tb_ced90c5d2e4444dba9facbc4a7bea9ac'
TB_THR_ID = 'tb_f831e270544947bfb38ec10300f4a104'
TB_XSZ_ID = 'tb_08849d2de54847e39ef4bc6f7112a00b'
TOKEN = 'aa3f911d91aa9351c6a7c36ab9198201'


def update():
    tb_ids = [TB_JSZ_ID, TB_THR_ID, TB_XSZ_ID]
    url = "{}/api/tb/update?access_token={}&tb_ids={}".format(URL, TOKEN, json.dumps(tb_ids))
    result = json.loads(requests.get(url).content)
    if not result['status'] == '0':
        # info().error('创建数据库失败:{}'.format(result['errstr']))
        raise ValueError
    print(result)
    # info().info('联级更新成功！')


if __name__ == '__main__':
    update()