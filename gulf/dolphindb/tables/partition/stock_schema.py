# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/10 9:29
# @Author   : Fangyang
# @Software : PyCharm

from gulf.dolphindb.const import DolType
from gulf.dolphindb.tables.partition.schema import new_schema, Schema


def stock_daily_table_schema() -> Schema:
    cols_type_dict = {'jj_code': DolType.SYMBOL, }

    # tushare daily 数据不全, 这里使用akshare数据
    daily_cols_type_dict = {
        'ts_code': DolType.SYMBOL, 'trade_date': DolType.NANOTIMESTAMP,
        # 'pre_close': DolType.DOUBLE,
        'open': DolType.DOUBLE, 'high': DolType.DOUBLE,
        'low': DolType.DOUBLE, 'close': DolType.DOUBLE,
        # 'change': DolType.DOUBLE,
        'pct_chg': DolType.DOUBLE, 'volume': DolType.DOUBLE,
        'amount': DolType.DOUBLE,
        # 'adj_pre_close': DolType.DOUBLE,
        'adj_open': DolType.DOUBLE, 'adj_high': DolType.DOUBLE, 'adj_low': DolType.DOUBLE,
        'adj_close': DolType.DOUBLE,
        # 'adj_factor': DolType.DOUBLE, 'avg_price': DolType.DOUBLE,
        # 'trade_status': DolType.SYMBOL,
    }

    # 由于akshare的 pe pb ps 数据质量没有tushare好, 所以这里都用tushare数据
    daily_basic_ts_cols_type_dict = {
        'ts_code': DolType.SYMBOL, 'trade_date': DolType.NANOTIMESTAMP,
        # 'close': DolType.DOUBLE,
        'turnover_rate': DolType.DOUBLE,
        'turnover_rate_f': DolType.DOUBLE,
        'volume_ratio': DolType.DOUBLE,
        'pe': DolType.DOUBLE, 'pe_ttm': DolType.DOUBLE,
        'pb': DolType.DOUBLE,
        'ps': DolType.DOUBLE, 'ps_ttm': DolType.DOUBLE,
        'dv_ratio': DolType.DOUBLE, 'dv_ttm': DolType.DOUBLE,
        'total_share': DolType.DOUBLE,
        'float_share': DolType.DOUBLE,
        'free_share': DolType.DOUBLE,
        'total_mv': DolType.DOUBLE, 'circ_mv': DolType.DOUBLE,
        # 'udlimit_status': DolType.INT   # 有 nan, pandas 转 int 报错
    }
    custom_dict = {'name': DolType.SYMBOL, 'industry': DolType.SYMBOL}

    cols_type_dict.update(daily_cols_type_dict)
    cols_type_dict.update(daily_basic_ts_cols_type_dict)
    cols_type_dict.update(custom_dict)

    columns_list = [
        list(daily_cols_type_dict.keys()),
        list(daily_basic_ts_cols_type_dict.keys()),
        list(custom_dict.keys()),
    ]

    return new_schema(cols_type_dict, columns_list)


def stock_moneyflow_daily_table_schema() -> Schema:
    cols_type_dict = {}

    # tushare daily 数据不全, 这里使用akshare数据
    daily_cols_type_dict = {
        'in_amount_main': DolType.DOUBLE, 'in_ratio_main': DolType.DOUBLE,
        'in_amount_large': DolType.DOUBLE, 'in_ratio_large': DolType.DOUBLE,
        'in_amount_big': DolType.DOUBLE, 'in_ratio_big': DolType.DOUBLE,
        'in_amount_mid': DolType.DOUBLE, 'in_ratio_mid': DolType.DOUBLE,
        'in_amount_small': DolType.DOUBLE, 'in_ratio_small': DolType.DOUBLE,
        'jj_code': DolType.SYMBOL, 'name': DolType.SYMBOL, 'industry': DolType.SYMBOL,
        'trade_date': DolType.NANOTIMESTAMP,
    }

    cols_type_dict.update(daily_cols_type_dict)

    columns_list = [
        list(daily_cols_type_dict.keys()),
    ]

    return new_schema(cols_type_dict, columns_list)


if __name__ == '__main__':
    pass
