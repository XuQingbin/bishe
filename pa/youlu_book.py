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
CATE_TAB = 'youlu_cate'
BOOK_TAB = 'youlu_book'

def get_urls(conn):
    sql = 'select id,url from %s where parent_id is not null'%CATE_TAB
    rows = conn.fetch(sql)
    return rows

def insert2db(conn,cid,bno,name,href,img,listprice,saleprice):
    try:
        bud = SqlQueryBuilder()
        bud.set_table(BOOK_TAB)
        bud.add_field('book_no',bno,'s')
        bud.add_field('book_name',name,'s')
        bud.add_field('cate',cid,'i')
        bud.add_field('create_time','now()','')
        bud.add_field('url',href,'s')
        bud.add_field('image_list',img,'s')
        bud.add_field('listprice',listprice,'f')
        bud.add_field('saleprice',saleprice,'f')
        sql = bud.get_insert()
        print sql
        print '-------------------------'
        conn.execute(sql)
    except Exception as e:
        print e
        time.slepp(3)

def get_html(url):
    try:
        res = urllib2.urlopen(url,timeout = 5)
        html = res.read().decode('gb18030')
        return html
    except Exception as e:
        print e
        time.sleep(3)
        return None

def parse_html(html,cid,conn):
    try:
        page = etree.HTML(html)
        lis=page.xpath("//form[@id='form1']/div[@class='main']/div[@id='classifyDefaultRight']/div[3]/ul/li")
        for li in lis:
            try:
                img=li.xpath('div[1]/a/img/@src')[0].encode('utf-8')
                href=li.xpath('div[1]/a/@href')[0].encode('utf-8')
                name=li.xpath('div[2]/div[1]/a/text()')[0].encode('utf-8')
                #info=li.xpath('div[2]/div[2]/text()')[0].encode('utf-8')
                #intro=li.xpath('div[2]/div[3]/text()')[0].encode('utf-8')
                listprice=li.xpath('div[2]/div[4]/span[1]/text()')[0]
                listprice=float(listprice[1:])
                saleprice=li.xpath('div[2]/div[4]/span[2]/text()')[0]
                saleprice=float(saleprice[1:])

                bno = href[1:]
                href = "http://www.youlu.net"+href

                insert2db(conn,cid,bno,name,href,img,listprice,saleprice)
                
            except Exception as e:
                typ, value, tb = sys.exc_info()
                print traceback.format_exception(typ, value, tb)
                time.sleep(3)
        nextag = page.xpath("//div[@class='pagerNewBookList']/span[@class='pagerMain']/*[last()]")
        if  nextag[0].tag == 'a':
            href = nextag[0].attrib['href']
            url = 'http://www.youlu.net'+href
            return url
        else:
            return None
    except Exception as e:
        typ, value, tb = sys.exc_info()
        print traceback.format_exception(typ, value, tb)
        time.sleep(10)

def get_conn():
    conn = SqlClient(host = DB_HOST,port = DB_PORT,user = DB_USER,
     passwd = DB_PWD)
    conn.set_db('oldbooks')
    return conn

def close_conn(conn):
    conn.close()

if __name__ == '__main__':
    conn = get_conn()
    cates = get_urls(conn)
    #close_conn(conn)

    count = 0
    flag = True
    for cate in cates:
        count += 1
        url = cate[1]
        cate_id = cate[0]
        if cate_id == 660 and flag == True:
            flag = False
        if flag == True:
            continue
        while url is not None:
            retry_time = 0
            while retry_time < 10:
                retry_time += 1
                html = get_html(url)
                if html is not None and len(html) > 500:
                    break
            if html is None or len(html) < 500:
                break
            url = parse_html(html,cate_id,conn)

    close_conn(conn)
