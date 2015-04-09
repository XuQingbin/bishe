# -*- coding: utf-8 -*-
# sql.py
# binli

'''wrapper mysql client'''
import MySQLdb
import MySQLdb.cursors

class SqlClient(object):

    def __init__(self, host="127.0.0.1", port=3306, 
                 user="root", passwd="root", charset="utf8"):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.charset = charset
        self.conn = MySQLdb.connect(
                host=self.host, port=self.port, user=self.user, 
                passwd=self.passwd, charset=self.charset)

    def __info__(self):
        info = {}
        info["host"] = self.host
        info["port"] = self.port
        info["user"] = self.user
        info["passwd"] = self.passwd
        info["charset"] = self.charset
        return info

    def __del__(self):
        # self.close()
        pass

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def set_db(self, db):
        self.conn.select_db(db)

    def fetch(self, sql, top=None):
        cur = self.conn.cursor()
        cur.execute(sql)
        if top is None:
            results = cur.fetchall()
        else:
            results = cur.fetchmany(top)
        self.conn.commit()
        cur.close()
        return results

    def dfetch(self, sql):
        """fetch rows in dict"""
        cur = self.conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        results = cur.fetchall()
        self.conn.commit()
        cur.close()
        return results

    def fetchmany(self, sql, num):
        '''diff with sql's fetchmany'''
        cur = self.conn.cursor()
        cur.execute(sql)
        results = cur.fetchall()
        self.conn.commit()
        cur.close()
        rows = list()
        index = 0
        for row in results:
            index += 1
            rows.append(row)
            if len(rows) == num or index == len(results):
                yield tuple(rows)
                del rows[:]

    def execute(self, sql):
        sqls = []
        if isinstance(sql, tuple):
            for s in sql:
                sqls.append(s)
        else:
            sqls.append(sql)
        try:
            cur = self.conn.cursor()
            for ele in sqls:
                cur.execute(ele)
            self.conn.commit()
            cur.close()
        except Exception as e:
            self.conn.rollback()
            raise Exception("TRANSACTION FAILED: " + str(e))
        del sqls[:]
        return True

    def executemany(self, sql, data):
        cur = self.conn.cursor()
        results = cur.executemany(sql, data)
        self.conn.commit()
        cur.close()
        return results
        

# end class SqlClient
