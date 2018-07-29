#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
import json
import pymysql
import ConfigParser
import os
from time import sleep, strptime, mktime, localtime
from time import time as nowtime

config = ConfigParser.ConfigParser()
FATHER_PATH = '/Users/zlt/small_demo/tupu'
FILE = os.path.join(FATHER_PATH, 'tupu_vaild_time.conf')
config.read(FILE)

MYSQL_CONFIG = {'host': config.get('MYSQL', 'host'),
                'user': config.get('MYSQL', 'user'),
                'pwd': config.get('MYSQL', 'pwd'),
                'database': config.get('MYSQL', 'database'),
                'port': config.getint('MYSQL', 'port')}
USER_ID = config.get('OTHERS', 'USER_ID')
FLAG = config.get('OTHERS', 'FLAG')
TIMESEELP = config.getint('OTHERS', 'TIMESEELP')


def init_mysql_connect():
    """
    链接mysql数据库
    :return: 游标，mysql链接
    """
    db = pymysql.connect(
        MYSQL_CONFIG['host'], MYSQL_CONFIG['user'], MYSQL_CONFIG['pwd'], MYSQL_CONFIG['database'], port=MYSQL_CONFIG['port'])
    cursor = db.cursor()
    return cursor, db


def vaild_ds_list(cursor, db, user_id=USER_ID):
    """
    通过flag筛选，返回所需要的数据源ds_id
    :return: list
    """
    vaild_ds_result = []
    sql = "SELECT ds_id FROM DS WHERE is_del = 0 and is_use = 1 and name REGEXP '^{}'".format(FLAG)
    if user_id:
        sql += " and owner = '{}'".format(user_id)
    cursor.execute(sql)
    data = cursor.fetchall()
    for item in data:
        vaild_ds_result.append(item[0])
    db.commit()
    return vaild_ds_result


def tassadar_api(cursor, ds_id, db):
    """
    通过ds_id访问tassadar库，返回该数据源status和utime
    :return: dict
    """
    for n in xrange(3):
        try:
            cursor.execute("SELECT status, utime FROM DS WHERE ds_id = '{}'".format(ds_id))
        except: # 怎么处理分开处理断开连接和服务器宕机情况
            sleep(1)
            pass
        else:
            break
    data = cursor.fetchone()
    db.commit()
    result = {'status': data[0], 'utime': str(data[1])}
    return result


def close(db):
    """
    关闭mysql链接
    """
    db.close()


def vaild_time(utime):
    """
    判断utime时间是否是今天，是返回True
    """
    ds_struct_time = strptime(utime, '%Y-%m-%d %H:%M:%S')
    ds_sec = mktime(ds_struct_time)

    now_sec = nowtime()
    now_struct_time = localtime(now_sec)

    if now_sec - ds_sec > 60*60*24:
        return False
    elif not ds_struct_time.tm_mday == now_struct_time.tm_mday:
        return False
    return True


def main():
    """
    主程序
    """
    cursor, db = init_mysql_connect()
    ds_queue = vaild_ds_list(cursor, db)
    start_time = nowtime()
    while True:
        if nowtime() - start_time > 60*60*24:
            return False
        ds_id = ds_queue.pop(0)
        ds_info = tassadar_api(cursor, ds_id, db)
        print ds_info
        if ds_info['status'] in (1, 0):
            if not vaild_time(ds_info['utime']):
                sleep(TIMESEELP*10)
                ds_queue.append(ds_id)
                print ('该数据源今天没有同步:{}，等待600s'.format(ds_id))
                # info().info('该数据源今天没有同步:{}，等待600s'.format(ds_id))
                continue
            else:
                # info().info('该数据源同步成功:{}'.format(ds_id))
                pass
        elif ds_info['status'] == 2:
            if not vaild_time(ds_info['utime']):
                sleep(TIMESEELP*10)
                ds_queue.append(ds_id)
                print ('该数据源今天没有同步:{}，等待600s'.format(ds_id))
                # info().info('该数据源上次同步失败，今天没有同步:{}，等待600s'.format(ds_id))
                continue
            else:
                # info().info('该数据源今日同步失败:{}'.format(ds_id))
                return False
        elif ds_info['status'] == 3:
            sleep(TIMESEELP)
            ds_queue.append(ds_id)
            # info().info('该数据源正在同步中:{}，等待60s'.format(ds_id))
            continue
        if len(ds_queue) == 0:
            break
    close(db)
    return True
        

print type(USER_ID.strip())
print '123'+ USER_ID.strip() + 'zlt'
