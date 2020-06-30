# !/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import sys
import getopt
from datetime import timedelta, datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')

from debt_analysis_algorithm.account_type_analysis import *
from debt_analysis_algorithm.enterprise_nature_analysis import *
from debt_analysis_algorithm.area_debt_risk_score import *


# 处理用户输入命令
def __get_input_batch_no_value(argv):
    batch_no = None
    host = None
    port = None
    database = None
    user = None
    password = None
    table = None
    model_code = None

    try:
        opts, args = getopt.getopt(argv, 'hb:H:P:d:u:p:t:m',
                                   ['help', 'batch_no=', 'host=', 'port=', 'database=', 'user=', 'password=', 'table=',
                                    "model="])
    except getopt.GetoptError:
        logger.error('enterprise_nature_analysis.py -b <batchNo> -H <host> -P <port> -d <databaseName> -u <username>'
                     ' -p <password> -t <tableName>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            logger.info('enterprise_nature_analysis.py -b <batchNo> -H <host> -P <port> -d <databaseName> -u <username>'
                        ' -p <password> -t <tableName>')
            sys.exit()
        elif opt in ("-b", "--batch_no"):
            batch_no = arg
        elif opt in ("-H", "--host"):
            host = arg
        elif opt in ("-P", "--port"):
            port = arg
        elif opt in ("-d", "--database"):
            database = arg
        elif opt in ("-u", "--user"):
            user = arg
        elif opt in ("-p", "--password"):
            password = arg
        elif opt in ("-t", "--table"):
            table = arg
        elif opt in ("-m", "--model"):
            model_code = arg

    if host is None or port is None or database is None or user is None or password is None:
        raise RuntimeError('参数错误，请使用 -h 查看帮助')

    if batch_no is None:
        batch_no = int((datetime.today() + timedelta(-1)).strftime('%Y%m%d%H%M%S'))

    return {
        'batch_no': batch_no,
        'host': host,
        'port': port,
        'database': database,
        'user': user,
        'password': password,
        'table': table,
        'model_code': model_code
    }


def main(argv):
    config_params = __get_input_batch_no_value(argv)
    logger.debug('batch_no: %s, model_code: %s', config_params['batch_no'], config_params['model_code'])
    print('batch_no: %s, model_code: %s', config_params['batch_no'], config_params['model_code'])

    # 企业性质分析
    try:
        if config_params['model_code'] is None or config_params['model_code'] == 'ENTERPRISE_NATURE_MODEL':
            logger.info('enterprise_nature_analysis start ...')
            enterprise_nature_analysis(config_params)
            logger.info('enterprise_nature_analysis end')
            print('response-status-%s' % 200)
    except BaseException as be:
        print(be)
        logger.error(be)

    # 账户分类
    try:
        if config_params['model_code'] is None or config_params['model_code'] == 'ACCOUNT_TYPE_MODEL':
            logger.info('account_type_analysis start ...')
            account_type_analysis(config_params)
            logger.info('account_type_analysis end')
            print('response-status-%s' % 200)
    except BaseException as be:
        logger.error(be)
        print('account-type-analysis-response-status-%s' % 500)

    # 区域债务风险评分
    try:
        if config_params['model_code'] is None or config_params['model_code'] == 'AREA_DEBT_RISK_SCORE':
            logger.info('area_debt_risk_score start ...')
            area_debt_risk_score(config_params)
            logger.info('area_debt_risk_score end')
            print('response-status-%s' % 200)
    except BaseException as be:
        logger.error(be)
        print('area-debt-risk-score-response-status-%s' % 500)

    # # 企业债务风险评分
    # try:
    #     if config_params['model_code'] is None or config_params['model_code'] == 'ENTERPRISE_DEBT_RISK_SCORE':
    #         logger.info('enterprise_debt_risk_score start ...')
    #         enterprise_debt_risk_score(config_params)
    #         logger.info('enterprise_debt_risk_score end')
    #         print('response-status-%s' % 200)
    # except BaseException as be:
    #     logger.error(be)
    #     print('account-type-analysis-response-status-%s' % 500)


if __name__ == '__main__':
    main(sys.argv[1:])
