#!/usr/bin/python
# -*- coding: utf-8 -*-

from main_jsz import main_jsz
from main_xsz import main_xsz
from main_thr import main_thr


while True:
    instruct = raw_input('请输入指令，退出请输入exit：')
    if instruct == 'jsz':
        main_jsz()
    elif instruct == 'xsz':
        main_xsz()
    elif instruct == 'thr':
        main_thr()
    elif instruct == 'exit':
        break
    else:
        print '指令错误！'