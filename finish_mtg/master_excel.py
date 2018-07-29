#!/usr/bin/python
# encoding:utf-8

import xlrd
import os
from config import config

excel_file_name = config.get('EXCEL', 'excel_file_name')

PATH = os.path.abspath(__file__)
PATH_START = PATH.find('/lib')
FATHER_PATH = PATH[0:PATH_START]

FILE = os.path.join(FATHER_PATH, excel_file_name)
EXCEL_ROW = config.getint('EXCEL', 'row')
EXCEL_COL = config.getint('EXCEL', 'column')

data = xlrd.open_workbook(FILE)  # 打开xls文件
table = data.sheets()[0]


def master_excel():
    global EXCEL_ROW
    result = table.col_values(EXCEL_COL, EXCEL_ROW, EXCEL_ROW+100)
    EXCEL_ROW += 100
    return result


if __name__ == '__main__':
    print master_excel()

