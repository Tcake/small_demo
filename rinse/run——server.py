#!/usr/bin/python
# encoding:utf-8

import ConfigParser
import os
from rinse import Rinse

cf = ConfigParser.ConfigParser()
FILE = os.path.join(os.getcwd(), 'rinse.conf')
cf.read(FILE)


for a_file in cf.sections():
    MAX_COL = cf.getint(a_file, 'max_col')
    MAX_ROW = cf.getint(a_file, 'max_row')
    COL_NAME = [int(i) for i in cf.get(a_file, 'name').split(',') if i]
    COL_NUM = [int(i) for i in cf.get(a_file, 'num').split(',') if i]

    rinse = Rinse(a_file, MAX_COL, MAX_ROW, COL_NAME, COL_NUM)
    rinse.main()

    os.remove('./data/' + a_file + '.xlsx')
    # res = raw_input('请输入指令：')
    # if res == 'exit':
    #     break
    # elif res == 'continue':
    #     continue
