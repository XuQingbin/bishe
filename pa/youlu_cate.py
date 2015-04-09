# -*- coding:utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import MySQLdb
from  lxml import etree
import urllib
import urllib2
from sqlbuilder import SqlQueryBuilder
from sqlcli import SqlClient

DB_HOST = 'waerbgcn.mysql.rds.aliyuncs.com'
DB_PORT = 3306
DB_USER = 'qingbin_xu'
DB_PWD = 'xds39766'
DB_DB = 'oldbooks'
TAB = 'youlu_cate'

def get_html(url):
    res = urllib2.urlopen(url)
    html = res.read().decode('gbk')
    return html

def insert2db(cate_p,cate_p_href,cate_name,cate_href):
    
    root_url = 'http://www.youlu.net'

    conn = SqlClient(host = DB_HOST,port = DB_PORT,user = DB_USER,
     passwd = DB_PWD)
    conn.set_db(DB_DB)

    bud = SqlQueryBuilder()
    bud.set_table(TAB)
    bud.add_field('cate_name',cate_p,'s')
    bud.add_field('create_time','now()','')
    bud.add_field('url',root_url+cate_p_href,'s')
    sql = bud.get_insert()
    bud.clear()
    print sql

    conn.execute(sql)
    
    maxid = conn.fetch('select max(id) from %s'%TAB)[0][0]
    parent_name = conn.fetch('select cate_name from %s where id  = %s'%(TAB,maxid))[0][0]

    print maxid

    lens = len(cate_name)
    for i in range(lens):
        bud.set_table(TAB)
        name = cate_name[i]
        href = root_url + cate_href[i]
        bud.add_field('cate_name',name,'s')
        bud.add_field('url',href,'s')
        bud.add_field('parent_id',maxid,'i')
        bud.add_field('parent_name',parent_name,'s')
        bud.add_field('create_time','now()','')
        sql = bud.get_insert()
        bud.clear()
        conn.execute(sql)
        
    conn.close()

def parse_html(html):
    root = etree.HTML(html)
    lis=root.xpath("//form[@id='form1']/div[@class='main']/div[@id='classifyDefaultRight']/div[2]/ul/li")
    for li in lis:
        cate_parent = li.xpath('./div[1]/a')[0].text
        cate_p_href = li.xpath('./div[1]/a/@href')[0]
        cate_name = li.xpath('./div[2]/a/text()')
        cate_href = li.xpath('./div[2]/a/@href')
        insert2db(cate_parent,cate_p_href,cate_name,cate_href)

if __name__ == '__main__':
    url = 'http://www.youlu.net/classify/'
    html = get_html(url)
    cates = parse_html(html)
