# import pymssql
# from dao.basesql import BaseSQL
import pyodbc
from log import MyLogger


class MSSQL:
    """
    对pymssql的简单封装
    pymssql库，该库到这里下载：http://www.lfd.uci.edu/~gohlke/pythonlibs/#pymssql
    使用该库时，需要在Sql Server Configuration Manager里面将TCP/IP协议开启
    """

    _logger = MyLogger.get_logger("datamngr")

    def __init__(self, host, user, pwd, db, charset="utf8", port="1433"):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.charset = charset
        self.port = port

    def _connect(self):
        """
        得到连接信息
        返回: conn.cursor()
        """
        if not self.db:
            MSSQL._logger.error('没有设置数据库信息')
            raise(NameError, "没有设置数据库信息")
        conn_str = 'DRIVER={SQL Server};DATABASE=%s;SERVER=%s;UID=%s;PWD=%s' % (
            self.db, self.host, self.user, self.pwd)
        self.conn = pyodbc.connect(conn_str, autocommit=True)
        # self.conn = pyodbc.connect(conn_str)
        cur = self.conn.cursor()
        if not cur:
            MSSQL._logger.error('连接数据库失败')
            raise(NameError, "连接数据库失败")
        else:
            return cur

    def find_all(self, sql):
        """
        执行查询语句
        返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段

        调用示例：
        ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
        resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
        for (id,NickName) in resList:
            print str(id),NickName
        """
        try:
            cur = self._connect()
            cur.execute(sql)
            resList = cur.fetchall()
            cur.close()
            # 查询完毕后必须关闭连接
            self.conn.close()
            return resList
        except Exception as e:
            MSSQL._logger.error("Execute Sql Failed! SQL: {}".format(sql))
            return None

    def find_all_as_dict(self, sql):
        """
        执行查询语句, 返回的每条记录是一个字典类型
        """
        try:
            cur = self._connect()
            cur.execute(sql)
            resList = cur.fetchall()
            columns = [column[0].upper() for column in cur.description]

            results = []
            for row in resList:
                results.append(dict(zip(columns, row)))
            cur.close()
            # 查询完毕后必须关闭连接
            self.conn.close()
            return results
        except Exception as e:
            MSSQL._logger.error("Execute Sql Failed! SQL: {}".format(sql))
            return None

    def find_first(self, sql, as_dict=False):
        """
        执行查询语句
        返回的是一个包含tuple记录行，tuple的元素是每行记录的字段
        """
        try:
            cur = self._connect()
            cur.execute(sql)
            row = cur.fetchone()
            result = row
            if as_dict:
                columns = [column[0].upper() for column in cur.description]
                result = dict(zip(columns, row))

            cur.close()
            # 查询完毕后必须关闭连接
            self.conn.close()
            return result
        except Exception as e:
            MSSQL._logger.error("Execute Sql Failed! SQL: {}".format(sql))
            return None

    def exec_nonquery(self, sql):
        """
        执行非查询语句

        调用示例：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        try:
            cur = self._connect()
            cur.execute(sql)
            cur.close()
            # self.conn.commit()
            self.conn.close()
        except Exception as e:
            MSSQL._logger.error("Execute Sql Failed! SQL: {}".format(sql))
            return False
        return True


if __name__ == "__main__":
    mssql = MSSQL("192.168.103.237", "sa", "cnki1234!", "cloudrepository")
    retval = mssql.find_all("SELECT vendor from extractconfig")
    print(retval)
