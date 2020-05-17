# coding:utf-8
import time
from debt_analysis_algorithm.auxiliary_function import *
from debt_analysis_algorithm.support.util.list_utils import list_partition


# 银行账户聚类
def __account_cluster(colname_values):
    account_model = load_model('model_files/kmeans_model.pkl')
    result = account_model.predict(colname_values)
    return result


# 企业性质批量数据结果返回函数
def __batch_account_result(config_params):
    cols = ['transaction90_num', 'income90_num', 'ex90_num', 'interval_num']

    trade_select_sql = "select * \
                        from ds_account_trade_info \
                        where batch_no = %s \
                        order by id" % config_params['batch_no']

    # 获取交易数据
    trade_data_list = panda_read_sql(config_params, trade_select_sql)
    # 获取模型计算所需数据
    model_data_list = load_model('model_files/account_std.pkl').transform(trade_data_list[cols])
    # 获取计算结果
    analysis_result_list = __account_cluster(model_data_list)

    result_list = []
    for i in range(len(analysis_result_list)):
        result_list.append({
            'enterprise_original_id': trade_data_list['company_id'][i],
            'uscc': trade_data_list['company_uscc'][i],
            'enterprise_name': trade_data_list['company_name'][i],
            'account_code': trade_data_list['bank_account'][i],
            'account_name': trade_data_list['account_name'][i],
            'open_bank': trade_data_list['bank'][i],
            'analysis_type': analysis_result_list[i]
        })

    return result_list


def __get_batch_insert_sql(table_name, batch_no, enterprise_nature_list):
    batch_insert_sql = "insert into %s(create_time, update_time, batch_no, enterprise_original_id, " \
                       "uscc, enterprise_name, account_code, account_name, open_bank, analysis_type) values " \
                       % (table_name or 'ar_account_analysis_type')
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    for en in enterprise_nature_list:
        batch_insert_sql += "('%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," \
                            % (current_time, current_time, batch_no, en['enterprise_original_id'], en['uscc'],
                               en['enterprise_name'], en['account_code'], en['account_name'], en['open_bank'],
                               en['analysis_type'])
    return batch_insert_sql[:-1] + " returning id"


def account_type_analysis(config_params):
    enterprise_nature_list = __batch_account_result(config_params)
    if enterprise_nature_list is None:
        return
    delete_by_sql(config_params, "delete from %s where batch_no = %s"
                  % ((config_params['table'] or 'ar_account_analysis_type'), config_params['batch_no']))
    for data_list in list_partition(enterprise_nature_list, 100):
        execute_sql(config_params, __get_batch_insert_sql(config_params['table'], config_params['batch_no'], data_list))


def main():
    pass


if __name__ == "__main__":
    main()

