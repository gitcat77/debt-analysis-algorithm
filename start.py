# coding:utf-8
import pandas as pd
from auxiliary_function import *
#import pandas as pd
# import LocalDebt.auxiliary_function
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

if __name__ == '__main__':
    cols = ['freq_bank_typebasic_Inner_y', 'sm_cp_typebasic_Expend_In_y', 'sum_comp_cont5_y',
            'freq_comp_cont5_y', 'sum_comp_cont5_mp', 'fqbk_typebasic_Expend_In_y',
            'freq_comp_all_Income_y', 'freq_comp_typemark_m', 'freq_comp_all_Expend_y',
            'sum_comp_cont5_m', 'freq_comp_cont5_m', 'freq_bank_cont3_m', 'sum_comp_cont5_yp',
            'sum_comp_typemark_y', 'freq_comp_typebasic_Expend_m', 'freq_bank_typemark_Expend_y',
            'freq_comp_typebasic_Expend_y', 'freq_comp_cont3_y', 'freq_comp_typemark_Expend_m',
            'freq_comp_typebasic_Income_y']
    sql = 'select %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s from model_company_basicinfo' % tuple(cols)

    db = Mssql(HOST, USER, PWD, DATABASE)
    print(type(db))
    # print(sql)
    # conn = db.connect()
    # rows = fill_null(conn,sql)
    # #print(len(rows))
    # r = enterprise_classify('model_files/company_classify.pkl',rows)
    # print(r)
