import datetime
import sys
import os
import configparser
import logging
import psycopg2
import base64

from DBUtils.PooledDB import PooledDB


class DatabaseOperator(object):
    """
    class for database operator
    """
    def __init__(self,
                 database_config_path, database_config=None):
        """
        Constructor
        """
        self._database_config_path = database_config_path

        # load database configuration
        if not database_config:
            self._database_config = self.parse_postgresql_config(database_config_path)
        else:
            self._database_config = database_config
        self._pool = None

    def database_config_empty(self):
        if self._database_config:
            return False
        else:
            return True

    def parse_postgresql_config(self, database_config_path=None):
        """解析pei数据库配置文件
            参数
        ---------
        arg1 : conf_file
                        数据库配置文件路径
            返回值
        --------
        dict
                        解析配置属性dict--config

            示例
        --------
                无
       """
        if database_config_path == None and self._database_config_path != None:
            database_config_path = self._database_config_path
        if not os.path.isfile(database_config_path):
            sys.exit("ERROR: Could not find configuration file: {0}".format(database_config_path))
        parser = configparser.SafeConfigParser()
        parser.read(database_config_path)
        config = {}
        config['database'] = parser.get('UniMonDB', 'Database')
        config['db_user'] = parser.get('UniMonDB', 'UserName')
        config['db_passwd'] = parser.get('UniMonDB', 'Password')
        config['db_port'] = parser.getint('UniMonDB', 'Port')
        config['db_host'] = parser.get('UniMonDB', 'Servername')
        self._database_config = config

        return config

    def get_pool_conn(self):
        if not self._pool:
            self.init_pgsql_pool()
        return self._pool.connection()

    def init_pgsql_pool(self):
        """利用数据库属性连接数据库
                    参数
                ---------
                arg1 : config
                                数据库配置属性
                    返回值
                --------

                    示例
                --------
                        无
        """
        # 字典config是否为空
        # config = self._database_config()
        POSTGREIP = self._database_config['db_host']
        POSTGREPORT = self._database_config['db_port']
        POSTGREDB = self._database_config['database']
        POSTGREUSER = self._database_config['db_user']
        POSTGREPASSWD = base64.b64decode(self._database_config['db_passwd']).decode()
        try:
            logging.info('Begin to create {0} postgresql pool on：{1}.\n'.format(POSTGREIP, datetime.datetime.now()))

            pool = PooledDB(
                creator=psycopg2,  # 使用链接数据库的模块mincached
                maxconnections=6,  # 连接池允许的最大连接数，0和None表示不限制连接数
                mincached=1,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
                maxcached=4,  # 链接池中最多闲置的链接，0和None不限制
                blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
                maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
                setsession=[],  # 开始会话前执行的命令列表。
                host=POSTGREIP,
                port=POSTGREPORT,
                user=POSTGREUSER,
                password=POSTGREPASSWD,
                database=POSTGREDB)
            self._pool = pool
            logging.info('SUCCESS: create postgresql success.\n')

        except Exception as e:
            logging.error('ERROR: create postgresql pool failed：{0}\n')
            self.close_db_cursor()
            sys.exit('ERROR: create postgresql pool error caused by {0}'.format(str(e)))

    def pg_select_operator(self, sql):
        """进行查询操作,函数返回前关闭cursor,conn
                    参数
                ---------
                arg1 : sql查询语句
                    返回值
                --------
                list:result
                                        类型为list的查询结果:result

                    示例
                --------
                        无
        """
        # 执行查询
        try:
            conn = self.get_pool_conn()
            cursor = conn.cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
        except Exception as e:
            logging.error('ERROR: execute  {0} causes error'.format(sql))
            sys.exit('ERROR: load data from database error caused {0}'.format(str(e)))
        finally:
            cursor.close()
            conn.close()
        return result

    def test_pool_con(self):
        sql = 'select * from ar_enterprise_nature limit 10'
        result = self.pg_select_operator(sql)
        print(result)

    def pg_insert_operator(self, sql):
        result = False
        try:
            conn = self.get_pool_conn()
            cursor = conn.cursor()
            cursor.execute(sql)
            result = True
        except Exception as e:
            logging.error('ERROR: execute  {0} causes error'.format(sql))
            sys.exit('ERROR: insert data from database error caused {0}'.format(str(e)))
        finally:
            cursor.close()
            conn.commit()
            conn.close()
        return result

    def pg_update_operator(self, sql):

        result = False
        try:
            conn = self.get_pool_conn()
            cursor = conn.cursor()
            cursor.execute(sql)
            result = True
        except Exception as e:
            logging.error('ERROR: execute  {0} causes error'.format(sql))
            sys.exit('ERROR: update data from database error caused {0}'.format(str(e)))
        finally:
            cursor.close()
            conn.commit()
            conn.close()
        return result

    def pg_delete_operator(self, sql):
        result = False
        # 执行查询
        try:
            conn = self.get_pool_conn()
            cursor = conn.cursor()
            cursor.execute(sql)
            result = True
        except Exception as e:
            logging.error('ERROR: execute  {0} causes error'.format(sql))
            sys.exit('ERROR: delete data from database error caused {0}'.format(str(e)))
        finally:
            cursor.close()
            conn.commit()
            conn.close()
        return result

    def close_pool(self):
        """关闭pool
                    参数
                ---------
                        无

                    返回值
                --------
                        无
                    示例
                --------
                        无
        """
        if self._pool != None:
            self._pool.close()


if __name__ == '__main__':
    # path = "C:\\Users\\user\\PycharmProjects\\debt-analysis-algorithm\\application.conf"
    # db = DatabaseOperator(database_config_path=path)
    # db.test_pool_con()
    db_config = {'database': 'debt-analysis', 'db_user': 'debt-analysis-algorithm', 'db_passwd': 'Bt701cF4D7f7Cyh$o', 'db_port': 5432, 'db_host': '47.116.106.157'}
    db = DatabaseOperator(database_config_path=None, database_config=db_config)
    db.test_pool_con()
