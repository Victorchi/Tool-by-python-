# -*- coding: utf-8 -*-
# auhtor by:Victor chi
import pymysql
from journal.dbtuils import MySQLHelper


class OperationMysql(object):
    def __init__(self):
        self.host = '192.168.103.237'
        self.user = 'cnki'
        self.password = 'cnki'
        self.database = 'clf'
        self.port = 3306
        self.encoding = 'utf8'
        self.db = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
            charset=self.encoding
        )
        self.cur = self.db.cursor()

    def find_all(self, sql):
        '''find data'''
        try:
            result = ''
            self.cur.execute(sql)
            result = self.cur.fetchall()
        except Exception as e:
            print (e)
            raise e
        finally:
            self.db.close()
        return result

    def insert(self,sql):
        '''insert data'''
        try:
            result = ''
            self.cur.execute(sql)
            # 提交
            print '----'
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise e
        finally:
            self.db.close()
        return result

    def update(self,sql):
        '''update data'''
        try:
            result = ''
            self.cur.execute(sql)
            # 提交
            print '----'
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise e
        finally:
            self.db.close()
        return result

    def delete(self,sql):
        '''delete data'''
        try:
            result = ''
            self.cur.execute(sql)
            # 提交
            print '----'
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise e
        finally:
            self.db.close()
        return result



if __name__ == '__main__':
    for i in range(10):
        sql = '''insert into test url VALUES ('dddddddd')'''
        print OperationMysql().insert(sql)
    # print MySQLHelper().find_all(sql)[0][0]
