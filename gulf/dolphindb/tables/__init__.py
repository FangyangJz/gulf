# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/10 9:10
# @Author   : Fangyang
# @Software : PyCharm

from gulf.dolphindb.db_path import DfsDbPath
from gulf.dolphindb.tables.dimension.bond_tables import BondRedeemTable, BondBasicTable, BondDelistTable, BondIndexTable
from gulf.dolphindb.tables.dimension.stock_tables import StockBasicTable, TradeCalenderTable

from gulf.dolphindb.tables.partition.stock_tables import (
    stock_daily_table, hk_hold_table, stock_fin_table,
    stock_pingji_table, stock_moneyflow_daily_table, industry_moneyflow_daily_table, stock_nfq_daily_table
)

from gulf.dolphindb.tables.partition.bond_tables import bond_daily_table

# 大写驼峰为 维度表, 小写为 分区表
db_table_map = {
    DfsDbPath.trade_calender: [TradeCalenderTable],

    DfsDbPath.stock_basic: [StockBasicTable],
    # DfsDbPath.stock_index: [IndexHS300MembersTable, IndexHS300DailyTable],
    DfsDbPath.stock_daily_code: [
        stock_nfq_daily_table,
        stock_daily_table,
        hk_hold_table,
        stock_fin_table,
        stock_pingji_table,
        stock_moneyflow_daily_table,
        industry_moneyflow_daily_table,
    ],
    DfsDbPath.stock_daily: [],

    DfsDbPath.bond_basic: [BondBasicTable, BondRedeemTable, BondDelistTable],
    DfsDbPath.bond_index: [BondIndexTable],
    DfsDbPath.bond_daily_code: [
        bond_daily_table,
    ]
}

if __name__ == '__main__':
    pass
