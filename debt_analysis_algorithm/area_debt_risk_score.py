# -*- coding: UTF-8 -*-

import time

from debt_analysis_algorithm.auxiliary_function import *
from debt_analysis_algorithm.support.util.list_utils import list_partition
from debt_analysis_algorithm.support.util.my_dbutils import DatabaseOperator


# 企业债务评分
def _area_risk_score(normal_data):
    fa_area = load_model('model_files/fa_area.pkl')
    fa_area.fit(normal_data)
    weight = fa_area.get_factor_variance()[1]
    factor_score = fa_area.transform(normal_data)
    score = (np.dot(factor_score, weight)/weight.sum()).real
    result = max_min_normalization(score)*100
    # result[result == 0] = 3
    # result[result == 1] = 1
    return result


# 获取地区债务风险基础信息
def __batch_area_base_info(config_params):
    cols = ['government_sex_debt_ratio', 'government_debt_ratio', 'implicit_deb_tratio', 'all_debt_service_ratio',
            'implicit_debt_service_ratio', 'government_debt_service_ratio', 'concerned_debt_service_ratio',
            'operational_debt_service_ratio', 'all_interest_expense_ratio', 'implicit_debt_interest_expense_ratio',
            'government_debt_interest_expense_ratio', 'concerned_debt_interest_expenseratio',
            'operational_debt_interest_expenseratio', 'implicit_debt_get_new_to_old_ratio',
            'all_statistics_debt_balance', 'government_statistics_debt_balance', 'implicit_statistics_debt_balance',
            'concerned_statistics_debt_balance', 'operational_statistics_debt_balance',
            'implicit_exclude_statistics_debt_balance']

    trade_select_sql = '''select *
                        from  ds_area_debt_risk_base_info
                        where batch_no = %s \
                        order by id
                        ''' % config_params['batch_no']
    # 获取交易数据
    trade_data_list = panda_read_sql(config_params, trade_select_sql)
    # 获取模型计算所需数据
    model_data_list = trade_data_list[cols]

    # 数据标准化
    normal_data = max_min_normalization(model_data_list)

    # 获取计算结果
    analysis_result_list = _area_risk_score(normal_data)

    result_list = []
    for i in range(len(analysis_result_list)):
        result_list.append({
            'area_id': trade_data_list['area_id'][i],
            'area_name': trade_data_list['parent_name'][i] + ' - ' + trade_data_list['area_name'][i],
            'parent_id': trade_data_list['parent_id'][i],
            'level_type': trade_data_list['level_type'][i],
            'score': analysis_result_list[i]
        })

    return result_list


def __get_batch_insert_sql(table_name, batch_no, enterprise_nature_list):
    batch_insert_sql = "insert into %s(create_time, update_time, batch_no, area_id, " \
                       "area_name, parent_id, level_type, score) values " % (table_name or 'ar_area_debt_risk')
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    for en in enterprise_nature_list:
        batch_insert_sql += "('%s', '%s', %s, '%s', '%s', '%s', '%s', %s)," \
                            % (current_time, current_time, str(batch_no),
                               str(en['area_id']), en['area_name'],
                               str(en['parent_id']), en['level_type'], str(en['score']))
    return batch_insert_sql[:-1] + " returning id"


def area_debt_risk_score(config_params):
    area_base_info_list = __batch_area_base_info(config_params)
    if area_base_info_list is None:
        return
    delete_by_sql(config_params, "delete from %s where batch_no = %s"
                  % ((config_params['table'] or 'ar_area_debt_risk'), config_params['batch_no']))

    db_config = {
        'database': config_params['database'],
        'db_user': config_params['user'],
        'db_passwd': config_params['password'],
        'db_port': config_params['port'],
        'db_host': config_params['host']
    }
    db = DatabaseOperator(database_config_path=None, database_config=db_config)

    for data_list in list_partition(area_base_info_list, 100):
        logger.info(__get_batch_insert_sql(config_params['table'], config_params['batch_no'], data_list))
        db.pg_insert_operator(__get_batch_insert_sql(config_params['table'], config_params['batch_no'], data_list))


def main():
    pass


if __name__ == "__main__":
    main()
