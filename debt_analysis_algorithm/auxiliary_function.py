# coding:utf-8
from sklearn.externals import joblib
import psycopg2
import pandas as pd
import base64
from debt_analysis_algorithm.common import *


class Postgre:
    def __init__(self, db_config):
        self.db_config = db_config

    # 查询方法
    def select(self, sql):
        logger.info(sql)
        conn = psycopg2.connect(database=self.db_config['database'], user=self.db_config['user'],
                                password=base64.b64decode(self.db_config['password']).decode(),
                                host=self.db_config['host'], port=self.db_config['port'])
        cur = conn.cursor()

        try:
            cur.execute(sql)
            rows = cur.fetchall()
            logger.info('select successfully')
            return rows
        except BaseException as be:
            logger.error(be)
        finally:
            try:
                cur.close()
            except BaseException as be:
                logger.error(be)
            try:
                conn.close()
            except BaseException as be:
                logger.error(be)

    # 插入方法
    def insert(self, sql):
        logger.info(sql)
        conn = psycopg2.connect(database=self.db_config['database'], user=self.db_config['user'],
                                password=base64.b64decode(self.db_config['password']).decode(),
                                host=self.db_config['host'], port=self.db_config['port'])
        cur = conn.cursor()

        try:
            cur.execute(sql)
            logger.info('insert successfully')
        except BaseException as be:
            logger.error(be)
        finally:
            try:
                cur.close()
            except BaseException as be:
                logger.error(be)
            try:
                conn.close()
            except BaseException as be:
                logger.error(be)


def load_model(path):
    """加载模型函数"""
    if os.path.isfile(os.path.dirname(os.path.realpath(sys.executable)) + "/" + path):
        return joblib.load(os.path.dirname(os.path.realpath(sys.executable)) + "/" + path)
    else:
        return joblib.load(sys.path[0] + "/" + path)


# 查询方法
def panda_read_sql(db_config, sql):
    logger.info(sql)
    conn = psycopg2.connect(database=db_config['database'], user=db_config['user'],
                            password=base64.b64decode(db_config['password']).decode(),
                            host=db_config['host'], port=db_config['port'])
    try:
        df = pd.read_sql(sql, conn)
        df = df.fillna(0)
        return df
    except BaseException as be:
        logger.error(be)
    finally:
        try:
            conn.close()
        except BaseException as be:
            logger.error(be)


# 插入方法
def execute_sql(db_config, sql):
    logger.info(sql)
    conn = psycopg2.connect(database=db_config['database'], user=db_config['user'],
                            password=base64.b64decode(db_config['password']).decode(),
                            host=db_config['host'], port=db_config['port'])
    cur = conn.cursor()

    try:
        logger.debug(sql)
        cur.execute(sql)
        conn.commit()
        logger.info('execute sql successfully')
        return cur.fetchone()
    except BaseException as be:
        logger.error(be)
        raise be
    finally:
        try:
            cur.close()
        except BaseException as be:
            logger.error(be)
        try:
            conn.close()
        except BaseException as be:
            logger.error(be)


# 删除方法
def delete_by_sql(db_config, sql):
    logger.info(sql)
    conn = psycopg2.connect(database=db_config['database'], user=db_config['user'],
                            password=base64.b64decode(db_config['password']).decode(),
                            host=db_config['host'], port=db_config['port'])
    cur = conn.cursor()

    try:
        logger.debug(sql)
        cur.execute(sql)
        conn.commit()
        logger.info('delete successfully')
    except BaseException as be:
        logger.error(be)
        raise be
    finally:
        try:
            cur.close()
        except BaseException as be:
            logger.error(be)
        try:
            conn.close()
        except BaseException as be:
            logger.error(be)


def main():
    pass


if __name__ == "__main__":
    main()
