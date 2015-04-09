# -*- coding: utf-8 -*-
# CopyRight 2013 eLong. All rights reserved
# sql_builder.py
# binli

import MySQLdb

'''vtype: s->string, i->int, l->long, f->float, t->datetime'''

class Field(object):

    def __init__(self, key, val, vtype='s'):
        self.key = key
        self.val = val
        self.vtype = vtype
        if self.val is None:
            self.val = ''
            self.vtype = 's'

    def getkey(self):
        return "`%s`" % MySQLdb.escape_string(str(self.key))

    def __str__(self):
        k = "`%s`" % MySQLdb.escape_string(str(self.key))
        v = MySQLdb.escape_string(str(self.val))
        if 's' == self.vtype:
            v = "'%s'" % v
        return "%s=%s" % (k, v)

# end class Field

class Condition(object):

    def __init__(self, key, operator, val, vtype='s'):
        self.key = key
        self.operator = operator
        self.val = val
        self.vtype = vtype
        if self.val is None:
            self.val = ''
            self.vtype = 's'

    def __str__(self):
        k = "`%s`" % MySQLdb.escape_string(str(self.key))
        v = MySQLdb.escape_string(str(self.val))
        if 's' == self.vtype:
            v = "'%s'" % v
        return "%s %s %s" % (k, self.operator, v)

# end class Condition

class SqlQueryBuilder(object):

    def __init__(self):
        self._table = ""
        self._fields = []
        self._conditions = []

    def __del__(self):
        self.clear()

    def clear(self):
        self._table = ""
        del self._fields[:]
        del self._conditions[:]
        return 0

    def set_table(self, table):
        self._table = table

    def get_table(self):
        return self._table

    def add_field(self, key, val, vtype='s'):
        f = Field(key, val, vtype)
        self._fields.append(f)
        return len(self._fields)

    def add_condition(self, key, op, val, vtype='s'):
        c = Condition(key, op, val, vtype)
        self._conditions.append(c)
        return len(self._conditions)

    def _get_kvlst(self):
        keys = ""
        vals = ""
        for f in self._fields:
            kv = str(f).split('=', 1)
            keys += (kv[0] + ", ")
            vals += (kv[1] + ", ")
        keys = '(' + keys.rstrip(", ") + ')'
        vals = '(' + vals.rstrip(", ") + ')'
        return (keys, vals)

    def _get_cdts(self, mcdtand=True):
        # mcdtand: multi conditions and or or?
        cdts = ""
        rela = " AND " if mcdtand else " OR "
        for cdt in self._conditions:
            cdts += (str(cdt) + rela)
        cdts = cdts.rstrip(rela)
        return cdts

    def _add_cdts(self, sql, mcdtand=True):
        sql = sql.rstrip(';')
        if len(self._conditions) > 0:
            cdts = self._get_cdts(mcdtand)
            sql += " WHERE " + cdts
        return (sql + ';')

    def get_select(self, mcdtand=True):
        sql = "SELECT %s FROM %s;"
        keys = ""
        for f in self._fields:
            keys += (f.getkey() + ", ")
        keys = keys.rstrip(", ")
        sql = sql % (keys, self._table)
        return self._add_cdts(sql, mcdtand)

    def get_update(self, mcdtand=True):
        sql = "UPDATE %s SET %s;"
        uplst = ""
        for f in self._fields:
            uplst += str(f) + ", "
        uplst = uplst.strip(", ")
        sql = sql % (self._table, uplst)
        return self._add_cdts(sql, mcdtand)

    def get_del(self, mcdtand=True):
        sql = "DELETE FROM %s;"
        sql = sql % self._table
        return self._add_cdts(sql, mcdtand)

    def get_insert(self):
        sql = "INSERT INTO %s %s VALUES %s;"
        keys, vals = self._get_kvlst()
        sql = sql % (self._table, keys, vals)
        return sql

    def get_replace(self):
        sql = "REPLACE INTO %s %s VALUES %s;"
        keys, vals = self._get_kvlst()
        sql = sql % (self._table, keys, vals)
        return sql

# end class SqlQueryBuilder
