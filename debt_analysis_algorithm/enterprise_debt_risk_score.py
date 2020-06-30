# -*- coding: UTF-8 -*-

import time
from sklearn.externals import joblib
from debt_analysis_algorithm.auxiliary_function import *

# 企业债务评分
def _conpany_risk_score(normal_data):
    fa_company = load_model('model_files/fa_company.pkl')
    fa_area.fit(normal_data)
    weight=fa_company.get_factor_variance()[1]
    factor_score =fa_company.transform(normal_data)
    score=(np.dot(factor_score,weight)/weight.sum()).real
    result=max_min_normalization(score)*100
    # result[result == 0] = 3
    # result[result == 1] = 1
    return result

# 企业债务评分数据结果返回函数
def __batch_enterprise_base_info(config_params):
    cols = ['sum_finance_money','sum_debt_balance','sum_operational_debt_finance_money','sum_implicit_debt_finance_money',
            'sum_operational_debt_debt_balance','sum_implicit_debt_debt_balance','sum_create_audit_finance_money',
            'sum_create_audit_debt_balance','sum_stock_debt_finance_money','sum_direct_finance_finance_money',
            'sum_others_finance_money','sum_direct_finance_debt_balance','sum_others_debt_balance','sum_withdraw_money',
            'current_assets_year_end_balance','current_assets_year_start_balance','not_current_assets_year_end_balance',
            'not_current_assets_year_start_balance','assets_total_year_end_balance','assets_total_year_start_balance',
            'liabilities_total_year_end_balance','liabilities_total_year_start_balance','total_owners_equity_year_end_balance',
            'total_liabilities_owners_equity_year_end_balance','total_liabilities_owners_equity_year_start_balance',
            'sum_account_balance_normal','sum_available_balance_normal','capital']

    forward = ['sum_finance_money','sum_debt_balance','sum_operational_debt_finance_money','sum_implicit_debt_finance_money',
            'sum_operational_debt_debt_balance','sum_implicit_debt_debt_balance','sum_create_audit_finance_money',
            'sum_create_audit_debt_balance','sum_stock_debt_finance_money','sum_direct_finance_finance_money',
            'sum_others_finance_money','sum_direct_finance_debt_balance','sum_others_debt_balance','sum_withdraw_money',
            'liabilities_total_year_end_balance','liabilities_total_year_start_balance']
    backward = ['current_assets_year_end_balance','current_assets_year_start_balance','not_current_assets_year_end_balance',
                'not_current_assets_year_start_balance', 'assets_total_year_end_balance','assets_total_year_start_balance',
                'total_owners_equity_year_end_balance','total_liabilities_owners_equity_year_end_balance',
                'total_liabilities_owners_equity_year_start_balance',
                'sum_account_balance_normal','sum_available_balance_normal','capital']

    trade_select_sql = '''select *
                        from  ds_company_debt_risk_base_info
                        where batch_no = %s \
                        order by id
                        ''' % config_params['batch_no']
    # 获取交易数据
    trade_data_list = panda_read_sql(config_params, trade_select_sql)
    # 获取模型计算所需数据
    model_data_list = trade_data_list[cols]

    # 数据标准化
    model_data_list[forward]= max_min_normalization(model_data_list[forward])
    model_data_list[backward]=min_max_normalization(model_data_list[backward])

    normal_data=model_data_list

    # 获取计算结果
    analysis_result_list = _company_risk_score(normal_data)

    result_list = []
    for i in range(len(analysis_result_list)):
        result_list.append({
            'enterprise_original_id': trade_data_list['company_id'][i],
            'uscc': trade_data_list['company_uscc'][i],
            'name': trade_data_list['company_name'][i],
            'score': analysis_result_list[i]
        })

    return result_list



def __get_batch_insert_sql(table_name, batch_no, enterprise_nature_list):
    batch_insert_sql = "insert into %s(create_time, update_time, batch_no, enterprise_original_id, " \
                       "uscc, name, score) values " % (table_name or 'ar_enterprise_debt_risk')
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    for en in enterprise_nature_list:
        batch_insert_sql += "('%s', '%s', %s, '%s', '%s', '%s', %s)," \
                            % (current_time, current_time, str(batch_no),
                               str(en['enterprise_original_id']), str(en['uscc']),
                               en['name'], str(en['score']))
    return batch_insert_sql[:-1] + " returning id"


def enterprise_debt_risk_score(config_params):
    enterprise_base_info_list = __batch_enterprise_base_info(config_params)
    if enterprise_base_info_list is None:
        return
    delete_by_sql(config_params, "delete from %s where batch_no = %s"
                  % ((config_params['table'] or 'ar_enterprise_debt_risk'), config_params['batch_no']))

    db_config = {
        'database': config_params['database'],
        'db_user': config_params['user'],
        'db_passwd': config_params['password'],
        'db_port': config_params['port'],
        'db_host': config_params['host']
    }
    db = DatabaseOperator(database_config_path=None, database_config=db_config)

    for data_list in list_partition(enterprise_nature_list, 100):
        db.pg_insert_operator(__get_batch_insert_sql(config_params['table'], config_params['batch_no'], data_list))


def main():
    pass


if __name__ == "__main__":
    main()
