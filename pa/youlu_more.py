# -*- coding:utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import time
import traceback
import urllib
import urllib2
from sqlcli import SqlClient
from sqlbuilder import SqlQueryBuilder
from lxml import etree

DB_HOST = 'waerbgcn.mysql.rds.aliyuncs.com'
DB_PORT = 3306
DB_USER = 'qingbin_xu'
DB_PWD = 'xds39766'
TAB = 'youlu_book'

def get_conn():
    conn = SqlClient(host = DB_HOST,port = DB_PORT,user = DB_USER,
     passwd = DB_PWD)
    conn.set_db('oldbooks')
    return conn

def close_conn(conn):
    conn.close()

def get_bnos(conn):
    sql = 'select id,book_no from %s'%TAB
    rows = conn.fetch(sql)
    return rows

def get_html(url):
    try:
        res = urllib2.urlopen(url,timeout = 5)
        html = res.read().decode('gb18030')
        return html
    except Exception as e:
        print e
        time.sleep(10)
        return None

def parse_html(bno):
    pass


if __name__ == '__main__':
    conn = get_conn()
    bnos = get_bnos(conn)
    for bno in bnos:
        print bno
    close_conn(conn)
