#!/usr/bin/python
# -*- coding: UTF-8 -*-

import urllib
import json
import requests

from opends_handelr import TB_JSZ_ID, TB_THR_ID, TB_XSZ_ID, commit, TOKEN, URL

WHERE = "`r_id`>=2400000 AND `r_id`<3200000"


def delete_data(tb_id):
    where_quote = urllib.quote(WHERE)
    url = "{}/api/data/bulkdelete?access_token={}&tb_id={}&where={}".format(URL, TOKEN, tb_id, where_quote)
    result = json.loads(requests.get(url).content)
    if not result['status'] == '0':
        print ('删除数据失败:{}'.format(result['errstr']))
        raise ValueError
    print ('success:{}'.format(result['errstr']))
    commit(tb_id)
    print "删除数据成功"


if __name__ == '__main__':
    delete_data(TB_THR_ID)
