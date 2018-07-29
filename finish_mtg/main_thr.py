#!/usr/bin/python
# -*- coding: utf-8 -*-

from Queue import Queue, Empty

from opends_handelr import thr_insert, commit, TB_THR_ID
from slave import thr_reader
from log import info
from collision import thr_collision
from config import config


LOG_NUMBER = config.getint('TB_THR', 'r_id')  # 需要设置,总数据行
PRODUCT_QUEUE = Queue(10)
THREAD_NUM = config.getint('TB_THR', 'thread_num')


def main_thr():
    log_count = LOG_NUMBER
    init_readers()
    while True:
        insert_data, flag = merge(log_count)
        answer = thr_insert(insert_data)
        if not answer == 'success':
            info().error('返回不成功,errstr:{}'.format(answer))
            info().info('插入数据不成功，logcount：{}'.format(log_count))
            raise ValueError
        if flag:
            break
        log_count += 10000
        info().info('计数器现在到了logcount:{}'.format(log_count))
    # close()
    commit(TB_THR_ID)


def merge(log_count):
    output_list = []
    count = 1
    empty_num = 0
    task_finish = False
    while count < 10001:
        now_count = log_count + count  # 这里是记录了从队列中获取数据的次数，可能api会返回nodata，可能数据会膨胀。
        try:
            data = PRODUCT_QUEUE.get(timeout=1)
        except Empty:
            if empty_num > 15:
                info().info('任务结束了')
                task_finish = True
                break
            else:
                info().info('Queue is empty!time：{}'.format(empty_num))
                empty_num += 1
                continue
        data_result = thr_collision(data, now_count)
        for item in data_result:
            output_list.append(item)
        count += 1
        if count % 1000 == 1:
            info().info('目前读取行数：{}'.format(now_count))
    return output_list, task_finish


def init_readers():
    reader_list = []
    for i in xrange(THREAD_NUM):
        reader_list.append(thr_reader(PRODUCT_QUEUE, i))  # 驾驶证reader
    for item in reader_list:
        item.start()


if __name__ == '__main__':
    main()
