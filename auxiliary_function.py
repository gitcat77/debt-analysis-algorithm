# coding:utf-8
import pymssql
import pandas as pd
from sklearn.externals import joblib

class Mssql:
    '''数据库连接类'''
    def __init__(self, host, user, pwd, database):
        """
        :param host: 主机
        :param user: 用户名
        :param pwd: 密码
        :param database: 数据库表
        """
        self.host = host
        self.user = user
        self.pwd = pwd
        self.database = database
    def connect(self):
        conn = pymssql.connect(self.host, self.user,
                               self.pwd, self.database)
        return conn
    def select(self, sql):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return rows
    def insert(self, sql):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(sql)
        print('Datas insert seccessfully!')

def load_model(path):
    '''加载模型函数'''
    model = joblib.load(path)
    return model

def dump_model(path, model):
    '''模型本地化函数'''
    joblib.dump(model, path)
    print('Model Saved Successfully!')


if __name__ == '__main__':
    HOST = '172.16.0.104'
    USER = 'analysis'
    PWD = '9o0p-[=]'
    DATABASE = 'AnalysisData'
    db_instance = Mssql(HOST, USER, PWD, DATABASE)
    conn = db_instance.connect()
    sql = 'select * from Model_Account_BasicInfo'
    df = pd.read_sql(sql, conn)
    print(df.head())
