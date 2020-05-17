# coding:utf-8
import pandas as pd
from debt_analysis_algorithm.auxiliary_function import *
#import pandas as pd
# import LocalDebt.auxiliary_function
import  numpy as np
HOST = '172.16.0.104'
USER = 'analysis'
PWD = '9o0p-[=]'
DATABASE = 'AnalysisData'
# db = Mssql(HOST, USER, PWD, DATABASE)
# print(type(db))

# 企业性质分类预测
def enterprise_classify(model_path, colname_values):
    company_model = load_model(model_path)
    result = company_model.predict(colname_values)
    return result

# 银行账户聚类
def account_cluster(model_path, colname_values):
    account_model = load_model(model_path)
    result = account_model.predict(colname_values)
    return result

def fill_null(conn,sql):
    df = pd.read_sql(sql,conn)
    df = df.fillna(0)
    return df

def result_insert(sql,database, user, pwd, host, port):#DATABASE, USER, PWD, HOST,PORT
    db = Postgre(database, user, pwd, host, port)
    db.insert(sql)
    print('Results insert Successfully!')
# 单个结果返回函数
def company_result(model_path, cols, database, user, pwd, host, port, batch_no,com_id):
    #model_sql = 'select %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s from ds_company_trade_info where batch_no = %s' % (tuple(cols),batch_no)
    db = Postgre(database, user, pwd, host, port)
    # row = db.select(sql)
    # df = pd.DataFrame()
    # df = pd.read_sql(sql,)
    #print(tuple(cols.extend([batch_no,com_id])))
    #print(cols.extend([batch_no,com_id]))
    #print(type(cols))
    format_sql = 'select %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s from ds_company_trade_info ' % (tuple(cols))
    latter_sql = "where batch_no=%s and company_id='%s'" %(batch_no,com_id)
    model_sql = format_sql + latter_sql
    row = list(db.select(model_sql)[0])
    # 空值补全为0
    for i in range(len(row)):
        if row[i] is None:
            row[i] = 0.0
    target = enterprise_classify(model_path, np.array(row).reshape(1,20))
    name_sql = "select company_name,company_uscc from ds_company_trade_info where company_id='%s' and batch_no=%s " %(com_id, batch_no)
    com_info = db.select(name_sql)[0]
    insert_sql = 'insert into ar_enterprise_nature(create_time,update_time,deleted,version,batch_no,enterprise_original_id,name,uscc,enterprise_nature) ' \
                 "VALUES (now(),now(),0,1,%s,'%s','%s','%s',%s)" % (batch_no, com_id, com_info[0], com_info[1], target[0])

    # Return the results to the database
    db.insert(insert_sql)


# 企业性质批量数据结果返回函数
def batch_company_result(model_path, cols, database, user, pwd, host, port, batch_no):
    db = Postgre(database, user, pwd, host, port)
    # format_sql = 'select %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s from ds_company_trade_info ' % (
    #     tuple(cols))
    # latter_sql = "where batch_no=%s and company_id='%s'" % (batch_no)
    # model_sql = format_sql + latter_sql
    # rows = db.select(model_sql)
    sql = "select * from ds_company_trade_info where batch_no=%s limit 10" % (batch_no)
    conn, cur = db.connect()
    #df = pd.read_sql(sql, conn)
    df = fill_null(conn, sql)
    model_datas = df[cols]
    target = enterprise_classify(model_path, model_datas)
    return df['company_id'].values.tolist(), target


def account_result(kmean_path,std_path,cols, database, user, pwd, host, port, batch_no):
    db = Postgre(database, user, pwd, host, port)
    format_sql = 'select %s,%s,%s,%s from ds_account_trade_info ' %tuple(cols)
    # latter_sql = "where batch_no=%s and bank_account=%s" % (batch_no, account_id)
    latter_sql = "where batch_no=%s " % (batch_no)
    model_sql = format_sql + latter_sql
    row = list(db.select(model_sql)[0])
    for i in range(len(row)):
        if row[i] is None:
            row[i] = 0.0
    std_row = load_model(std_path).transform(np.array(row).reshape(1,4))
    target = account_cluster(kmean_path, std_row)

    insert_sql = 'insert into ar_account_analysis_type(create_time,update_time,deleted,version,batch_no,enterprise_original_id,enterprise_name,account_name,analysis_type) ' \
                 "VALUES (now(),now(),0,1,%s,'test111','test','nameaccount1223',%s)" % (batch_no, target[0])

    db.insert(insert_sql)

# 账户性质批量数据结果返回函数
def batch_account_results(kmean_path,std_path,cols, database, user, pwd, host, port, batch_no):
    db = Postgre(database, user, pwd, host, port)
    sql = 'select * from ds_account_trade_info where batch_no=%s limit 10' % (batch_no)
    conn, cur = db.connect()
    df = fill_null(conn, sql)
    model_datas = df[cols].values
    std_row = load_model(std_path).transform(model_datas)
    target = account_cluster(kmean_path, std_row)
    return df['company_id'].values.tolist(), target



if __name__ == '__main__':
    cols = ['freq_bank_typebasic_inner_y', 'sm_cp_typebasic_expend_in_y', 'sum_comp_cont5_y',
            'freq_comp_cont5_y', 'sum_comp_cont5_mp', 'fqbk_typebasic_expend_in_y',
            'freq_comp_all_income_y', 'freq_comp_typemark_m', 'freq_comp_all_expend_y',
            'sum_comp_cont5_m', 'freq_comp_cont5_m', 'freq_bank_cont3_m', 'sum_comp_cont5_yp',
            'sum_comp_typemark_y', 'freq_comp_typebasic_expend_m', 'freq_bank_typemark_expend_y',
            'freq_comp_typebasic_expend_y', 'freq_comp_cont3_y', 'freq_comp_typemark_expend_m',
            'freq_comp_typebasic_income_y']
    # sql = 'select %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s from model_company_basicinfo' % tuple(cols)
    #
    # db = Mssql(HOST, USER, PWD, DATABASE)
    # rows = db.select(sql)
    # print(type(db))
    # print(sql)
    # conn = db.connect()
    # rows = fill_null(conn,sql)
    # #print(len(rows))
    # r = enterprise_classify('model_files/company_classify.pkl',rows)
    # print(r)
    HOST = '47.103.86.17'
    PWD = 'Bt701cF4D7f7Cyh$o'
    USER = 'debt-analysis-algorithm'
    PORT = 5432
    DATABASE = 'debt-analysis'
    #company_result('model_files/company_classify.pkl', cols, DATABASE, USER, PWD, HOST, PORT,20200218145507,'A342B28A-8693-4F4A-B695-4A03BB4C433B')
    account_clos =['transaction90_num','income90_num','ex90_num', 'interval_num']
    #account_result('model_files/kmeans_model.pkl','model_files/account_std.pkl', account_clos,DATABASE, USER, PWD, HOST, PORT,20200218145507)
    # id, target, batch_no = batch_company_result('model_files/company_classify.pkl', cols, DATABASE, USER, PWD, HOST, PORT, 20200218145507)
    # print(id, target, batch_no)
    id, target = batch_account_results('model_files/kmeans_model.pkl','model_files/account_std.pkl', account_clos,DATABASE, USER, PWD, HOST, PORT,20200218145507)
    print(id, target)