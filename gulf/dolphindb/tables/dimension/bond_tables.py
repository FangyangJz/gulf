# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/10 9:40
# @Author   : Fangyang
# @Software : PyCharm
import pandas as pd

from gulf.dolphindb.db_path import DfsDbPath
from gulf.dolphindb.tables.dimension.table import DimensionTable


class BondBasicTable(DimensionTable):
    name = "bond_basic_table"
    db_path = DfsDbPath.bond_basic
    db_name = "bond_basic_db"
    sort_columns = 'jj_code'

    def __init__(self):
        super().__init__()

        from gulf.akshare.bond import get_bond_basic_df

        table_df, _ = get_bond_basic_df()
        table_df['上市时间'] = pd.to_datetime(table_df['上市时间'])
        table_df['转股起始日'] = pd.to_datetime(table_df['转股起始日'])

        self.df = table_df


class BondRedeemTable(DimensionTable):
    name = "bond_redeem_table"
    db_path = DfsDbPath.bond_basic
    db_name = "bond_basic_db"
    sort_columns = 'jj_code'

    def __init__(self):
        super().__init__()

        from gulf.akshare.bond import get_bond_basic_df

        _, redeem_table_df = get_bond_basic_df()
        redeem_table_df['转股起始日'] = pd.to_datetime(redeem_table_df['转股起始日'])

        self.df = redeem_table_df


class BondDelistTable(DimensionTable):
    name = "bond_delist_table"
    db_path = DfsDbPath.bond_basic
    db_name = 'bond_basic_db'
    sort_columns = 'jj_code'

    def __init__(self):
        """
        117*** 是 EB可交换债, 正股代码 和 名称 为 None
        """
        super(BondDelistTable, self).__init__()

        from avalon.datafeed.xcsc_tushare import get_delist_bond_df

        table_df = get_delist_bond_df()
        table_df['trade_dt_last'] = pd.to_datetime(table_df['trade_dt_last'])
        self.df = table_df


class BondIndexTable(DimensionTable):
    name = "bond_index_table"
    db_path = DfsDbPath.bond_index
    db_name = 'bond_index_db'
    sort_columns = 'index'

    def __init__(self):
        super(BondIndexTable, self).__init__()

        from gulf.akshare.bond import get_bond_index_daily

        table_df = get_bond_index_daily()
        table_df['trade_date'] = pd.to_datetime(table_df['trade_date'])
        table_df.reset_index(inplace=True)
        self.df = table_df


if __name__ == '__main__':
    pass
