#!/usr/bin/python
# encoding:utf-8
"""
日志管理系统
"""
import logging
import os
from config import config


logfile_name = config.get('LOG', 'logfile_name')
PATH = os.path.abspath(__file__)
PATH_START = PATH.find('/lib')
FATHER_PATH = PATH[0:PATH_START]
FILE = os.path.join(FATHER_PATH, logfile_name)

logger = logging.getLogger('runtime')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(name)s:' '%(levelname)s: %(message)s')

hdr = logging.StreamHandler()
fhr = logging.FileHandler(FILE)
hdr.setFormatter(formatter)
fhr.setFormatter(formatter)

logger.addHandler(hdr)
logger.addHandler(fhr)


def info():
    return logger


if __name__ == '__main__':
    info().info('hello,word!')
