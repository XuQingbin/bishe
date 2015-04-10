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
    sql = 'select id,book_no,url from %s'%TAB
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

def parse_html(html):
    root = etree.HTML(html)
    main = root.xpath("//div[@class='main']/div[@class='bi3Top']/div[@class='bi3TopLeft']/div[@class='info4']")[0]
    div1 = main.xpath('./div[1]')[0]
    isbnplus = div1.xpath('string(.)')

    author = main.xpath('./div[2]/a[1]/text()')[0]
    press = main.xpath('./div[2]/a[2]/text()')[0]

    div3 = main.xpath('./div[3]')[0]
    bookmore = div3.xpath('string(.)')

    print isbnplus
    print author
    print press
    print bookmore
    print '----------------'

    #print main[0].tag
    #print main[0]


if __name__ == '__main__':
    conn = get_conn()
    bnos = get_bnos(conn)
    for bno in bnos:
        html = get_html(bno[2])
        parse_html(html)
    close_conn(conn)
