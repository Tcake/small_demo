#!/usr/bin/python
# -*- coding: utf-8 -*-

from main_jsz import main_jsz
from main_xsz import main_xsz
from main_thr import main_thr
from opends_handelr import update
from time import sleep


while True:
    instruct = raw_input('请输入指令，退出请输入exit：')
    if instruct == 'jsz':
        main_jsz()
    elif instruct == 'xsz':
        main_xsz()
    elif instruct == 'thr':
        main_thr()
    elif instruct == 'exit':
        update()
        print('退出倒计时')
        for i in range(10, 0, -1):
            sleep(1)
            print(i)
        break
    else:
        print '指令错误！'