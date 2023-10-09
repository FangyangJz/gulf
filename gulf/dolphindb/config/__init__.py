from gulf.dolphindb.config.db_path import DfsDbPath
from gulf.dolphindb.config.dimension_table import (
    TradeCalenderTable, StockBasicTable, BondBasicTable, BondDelistTable,
    IndexHS300MembersTable, IndexHS300DailyTable, BondIndexTable, BondRedeemTable
)
from gulf.dolphindb.config.partition_table import (
    stock_daily_table, hk_hold_table, bond_daily_table, stock_fin_table,
    stock_pingji_table, stock_moneyflow_daily_table, industry_moneyflow_daily_table
)

# 大写驼峰为 维度表, 小写为 分区表
db_table_map = {
    DfsDbPath.trade_calender: [TradeCalenderTable],

    DfsDbPath.stock_basic: [StockBasicTable],
    DfsDbPath.stock_index: [IndexHS300MembersTable, IndexHS300DailyTable],
    DfsDbPath.stock_daily_code: [
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
