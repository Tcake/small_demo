#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymysql

# 打开数据库连接
db = pymysql.connect("127.0.0.1", "root", "", "test", charset='utf8')

# 使用cursor()方法获取操作游标
cursor = db.cursor()

# 使用execute方法执行SQL语句
cursor.execute("SELECT renlei FROM my_test ORDER BY renlei")


# 使用 fetchone() 方法获取一条数据
def master_database():
    data = cursor.fetchmany(100)  # type data: ((2L,), (3L,), (4L,), (9L,))
    # cursor.scroll(count, mode='relative')
    result = []
    for item in data:
        result.append(item[0])
    return result  # type result: [2L, 3L, 4L, 9L]


# 关闭数据库连接
def close():
    db.commit()
    cursor.close()
    db.close()


if __name__ == '__main__':
    print master_database()
    close()
