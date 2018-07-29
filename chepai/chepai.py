#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
import re
import pymysql


def mysql_connect():
    db = pymysql.connect('127.0.0.1', 'haizhioffdb', 'b@30QpqPkh', 'tassadar', port=4386, charset='utf8')
    cursor = db.cursor()
    return db, cursor


def call_url():
    url = 'http://www.chebiaow.com/chepai/'
    result = requests.get(url).content
    return result


def handle_str(content, cursor):
    province_list = re.findall("caption id(?:.|\n)*?table", content)
    for province in province_list:
        province_name = re.search('"">(.*)</a></caption>', province).groups()[0]
        for item in re.findall('">(.*)</a></td>(?:.|\n)*?<td>(.*)</td>', province):
            sql = "INSERT INTO CHEPAI(paihao,province,city) VALUES ('{}','{}','{}');".format(item[0], province_name, item[1])
            cursor.execute(sql)


def main():
    content = call_url()
    db, cursor = mysql_connect()
    handle_str(content, cursor)
    db.commit()
    db.close()


main()

