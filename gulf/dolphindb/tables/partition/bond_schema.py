# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/10 9:29
# @Author   : Fangyang
# @Software : PyCharm
from gulf.dolphindb.const import DolType
from gulf.dolphindb.tables.partition.schema import Schema, new_schema


def bond_daily_table_schema() -> Schema:
    """
    ['jj_code', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'bond_code',
   'bond_name', 'stock_code', 'stock_name', 'bond_scale', 'listing_date',
   'market', 'bond_value', 'trans_stock_value', 'bond_premium',
   'trans_stock_premium', 'duallow']

    :return: None
    """

    bond_daily_cols_type_dict = {
        'jj_code': DolType.SYMBOL, 'trade_date': DolType.NANOTIMESTAMP,
        'open': DolType.DOUBLE, 'high': DolType.DOUBLE, 'low': DolType.DOUBLE, 'close': DolType.DOUBLE,
        'amount': DolType.DOUBLE, 'volume': DolType.DOUBLE,
        'stock_open': DolType.DOUBLE, 'stock_high': DolType.DOUBLE, 'stock_low': DolType.DOUBLE,
        'stock_close': DolType.DOUBLE, 'stock_amount': DolType.DOUBLE, 'stock_volume': DolType.DOUBLE,
        'bond_code': DolType.SYMBOL, 'bond_name': DolType.SYMBOL,
        'stock_code': DolType.SYMBOL, 'stock_name': DolType.SYMBOL,
        'bond_scale': DolType.DOUBLE, 'listing_date': DolType.NANOTIMESTAMP,
        'market': DolType.SYMBOL, 'trans_stock_value': DolType.DOUBLE,
        # 'bond_value': DolType.DOUBLE,  'bond_premium': DolType.DOUBLE,
        'trans_stock_premium': DolType.DOUBLE, 'duallow': DolType.DOUBLE
    }

    cols_type_dict = dict()
    cols_type_dict.update(bond_daily_cols_type_dict)

    columns_list = [list(bond_daily_cols_type_dict.keys())]

    return new_schema(cols_type_dict, columns_list)


if __name__ == '__main__':
    pass
