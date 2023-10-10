# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/10 8:29
# @Author   : Fangyang
# @Software : PyCharm

from gulf.dolphindb.db_path import DfsDbPath
from gulf.dolphindb.tables.partition.schema import industry_moneyflow_daily_table_schema, index_daily_table_schema, \
    hk_hold_table_schema
from gulf.dolphindb.tables.partition.stock_schema import stock_daily_table_schema, stock_moneyflow_daily_table_schema
from gulf.dolphindb.tables.partition.table import PartitionTable


stock_daily_table = PartitionTable(
    name="stock_daily_table",
    db_path=DfsDbPath.stock_daily_code,
    schema=stock_daily_table_schema()
)

stock_moneyflow_daily_table = PartitionTable(
    name="stock_moneyflow_daily_table",
    db_path=DfsDbPath.stock_daily_code,
    schema=stock_moneyflow_daily_table_schema()
)

industry_moneyflow_daily_table = PartitionTable(
    name="industry_moneyflow_daily_table",
    db_path=DfsDbPath.stock_daily_code,
    schema=industry_moneyflow_daily_table_schema()
)

index_concept_daily_table = PartitionTable(
    name="index_concept_daily_table",
    db_path=DfsDbPath.index_daily_code,
    schema=index_daily_table_schema()
)

index_industry_daily_table = PartitionTable(
    name="index_industry_daily_table",
    db_path=DfsDbPath.index_daily_code,
    schema=index_daily_table_schema()
)

hk_hold_table = PartitionTable(
    name="hk_hold_table",
    db_path=DfsDbPath.stock_daily_code,
    schema=hk_hold_table_schema()
)

# dfs分区数据库, 建完库以后, 数据表通过python api 写入,
# 根据 dataframe 结构动态定义 schema
stock_moneyflow_hsgt_table = PartitionTable(
    name="moneyflow_hsgt_table",
    db_path=DfsDbPath.stock_daily,
    schema=None
)

stock_fin_table = PartitionTable(
    name="fin_table",
    db_path=DfsDbPath.stock_daily_code,
    schema=None
)

stock_pingji_table = PartitionTable(
    name="pingji_table",
    db_path=DfsDbPath.stock_daily_code,
    schema=None
)

if __name__ == '__main__':
    pass
