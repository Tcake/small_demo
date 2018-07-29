#!/usr/bin/python
# encoding:utf-8

import ConfigParser
import os

cf = ConfigParser.ConfigParser()

PATH = os.path.abspath(__file__)
PATH_START = PATH.find('/lib')
FATHER_PATH = PATH[0:PATH_START]
FILE = os.path.join(FATHER_PATH, 'MTG.conf')
cf.read(FILE)

config = cf
