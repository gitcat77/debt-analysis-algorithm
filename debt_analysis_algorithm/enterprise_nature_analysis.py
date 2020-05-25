# coding:utf-8
import time
from debt_analysis_algorithm.auxiliary_function import *
from debt_analysis_algorithm.support.util.list_utils import list_partition
from debt_analysis_algorithm.support.util.my_dbutils import DatabaseOperator


# 企业性质分类预测
def _enterprise_classify(colname_values):
    company_model = load_model('model_files/company_classify.pkl')
    result = company_model.predict(colname_values)
    return result


# 企业性质批量数据结果返回函数
def __batch_company_result(config_params):
    cols = ['freq_bank_typebasic_inner_y', 'sm_cp_typebasic_expend_in_y', 'sum_comp_cont5_y',
            'freq_comp_cont5_y', 'sum_comp_cont5_mp', 'fqbk_typebasic_expend_in_y',
            'freq_comp_all_income_y', 'freq_comp_typemark_m', 'freq_comp_all_expend_y',
            'sum_comp_cont5_m', 'freq_comp_cont5_m', 'freq_bank_cont3_m', 'sum_comp_cont5_yp',
            'sum_comp_typemark_y', 'freq_comp_typebasic_expend_m', 'freq_bank_typemark_expend_y',
            'freq_comp_typebasic_expend_y', 'freq_comp_cont3_y', 'freq_comp_typemark_expend_m',
            'freq_comp_typebasic_income_y']

    trade_select_sql = "select company_id, company_uscc, company_name, \
                            freq_bank_typebasic_inner_y, sm_cp_typebasic_expend_in_y, sum_comp_cont5_y, \
                            freq_comp_cont5_y, sum_comp_cont5_mp, fqbk_typebasic_expend_in_y, \
                            freq_comp_all_income_y, freq_comp_typemark_m, freq_comp_all_expend_y, \
                            sum_comp_cont5_m, freq_comp_cont5_m, freq_bank_cont3_m, sum_comp_cont5_yp, \
                            sum_comp_typemark_y, freq_comp_typebasic_expend_m, freq_bank_typemark_expend_y, \
                            freq_comp_typebasic_expend_y, freq_comp_cont3_y, freq_comp_typemark_expend_m, \
                            freq_comp_typebasic_income_y \
                        from ds_company_trade_info \
                        where batch_no = %s \
                        order by id" % config_params['batch_no']

    # 获取交易数据
    trade_data_list = panda_read_sql(config_params, trade_select_sql)
    # 获取模型计算所需数据
    model_data_list = trade_data_list[cols]
    # 获取计算结果
    analysis_result_list = _enterprise_classify(model_data_list)

    result_list = []
    for i in range(len(analysis_result_list)):
        result_list.append({
            'enterprise_original_id': trade_data_list['company_id'][i],
            'uscc': trade_data_list['company_uscc'][i],
            'name': trade_data_list['company_name'][i],
            'enterprise_nature': analysis_result_list[i]
        })

    return result_list


def __get_batch_insert_sql(table_name, batch_no, enterprise_nature_list):
    batch_insert_sql = "insert into %s(create_time, update_time, batch_no, enterprise_original_id, " \
                       "uscc, name, enterprise_nature) values " % (table_name or 'ar_enterprise_nature')
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    for en in enterprise_nature_list:
        batch_insert_sql += "('%s', '%s', %s, '%s', '%s', '%s', %s)," \
                            % (current_time, current_time, batch_no,
                               en['enterprise_original_id'], en['uscc'],
                               en['name'], en['enterprise_nature'])
    return batch_insert_sql[:-1] + " returning id"


def enterprise_nature_analysis(config_params):
    enterprise_nature_list = __batch_company_result(config_params)
    if enterprise_nature_list is None:
        return
    delete_by_sql(config_params, "delete from %s where batch_no = %s"
                  % ((config_params['table'] or 'ar_enterprise_nature'), config_params['batch_no']))

    db_config = {
        'database': config_params['database'],
        'db_user': config_params['user'],
        'db_passwd': config_params['password'],
        'db_port': config_params['port'],
        'db_host': config_params['host']
    }
    db = DatabaseOperator(database_config_path=None, database_config=db_config)

    for data_list in list_partition(enterprise_nature_list, 10):
        db.pg_insert_operator(__get_batch_insert_sql(config_params['table'], config_params['batch_no'], data_list))


def main():
    pass


if __name__ == "__main__":
    main()

