# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/10 8:29
# @Author   : Fangyang
# @Software : PyCharm
from gulf.dolphindb.const import KeepDuplicate
from gulf.dolphindb.db_path import DfsDbPath
from gulf.dolphindb.tables.partition.schema import industry_moneyflow_daily_table_schema, index_daily_table_schema, \
    hk_hold_table_schema
from gulf.dolphindb.tables.partition.stock_schema import stock_daily_table_schema, stock_moneyflow_daily_table_schema, \
    stock_nfq_daily_table_schema
from gulf.dolphindb.tables.partition.table import PartitionTable

stock_nfq_daily_table = PartitionTable(
    name="stock_nfq_daily_table",
    db_path=DfsDbPath.stock_daily_code,
    schema=stock_nfq_daily_table_schema(),
    partition_columns=['jj_code', 'trade_date'],
    sort_columns=['jj_code', 'trade_date'],
    keep_duplicates=KeepDuplicate.FIRST
)

stock_daily_table = PartitionTable(
    name="stock_daily_table",
    db_path=DfsDbPath.stock_daily_code,
    schema=stock_daily_table_schema(),
    partition_columns=['jj_code', 'trade_date'],
    sort_columns=['jj_code', 'trade_date'],
    keep_duplicates=KeepDuplicate.FIRST
)

stock_moneyflow_daily_table = PartitionTable(
    name="stock_moneyflow_daily_table",
    db_path=DfsDbPath.stock_daily_code,
    schema=stock_moneyflow_daily_table_schema(),
    partition_columns=['jj_code', 'trade_date'],
    sort_columns=['jj_code', 'trade_date'],
    keep_duplicates=KeepDuplicate.FIRST
)

industry_moneyflow_daily_table = PartitionTable(
    name="industry_moneyflow_daily_table",
    db_path=DfsDbPath.stock_daily_code,
    schema=industry_moneyflow_daily_table_schema(),
    partition_columns=['jj_code', 'trade_date'],
    sort_columns=['jj_code', 'trade_date'],
    keep_duplicates=KeepDuplicate.FIRST
)

index_concept_daily_table = PartitionTable(
    name="index_concept_daily_table",
    db_path=DfsDbPath.index_daily_code,
    schema=index_daily_table_schema(),
    partition_columns=['jj_code', 'trade_date'],
    sort_columns=['jj_code', 'trade_date'],
    keep_duplicates=KeepDuplicate.FIRST
)

index_industry_daily_table = PartitionTable(
    name="index_industry_daily_table",
    db_path=DfsDbPath.index_daily_code,
    schema=index_daily_table_schema(),
    partition_columns=['jj_code', 'trade_date'],
    sort_columns=['jj_code', 'trade_date'],
    keep_duplicates=KeepDuplicate.FIRST
)

hk_hold_table = PartitionTable(
    name="hk_hold_table",
    db_path=DfsDbPath.stock_daily_code,
    schema=hk_hold_table_schema(),
    partition_columns=['jj_code', 'trade_date'],
    sort_columns=['jj_code', 'trade_date'],
    keep_duplicates=KeepDuplicate.FIRST
)

# dfs分区数据库, 建完库以后, 数据表通过python api 写入,
# 根据 dataframe 结构动态定义 schema
stock_moneyflow_hsgt_table = PartitionTable(
    name="moneyflow_hsgt_table",
    db_path=DfsDbPath.stock_daily,
    schema=None,
    partition_columns=['jj_code', 'trade_date'],
    sort_columns=['jj_code', 'trade_date'],
    keep_duplicates=KeepDuplicate.FIRST
)

stock_fin_table = PartitionTable(
    name="fin_table",
    db_path=DfsDbPath.stock_daily_code,
    schema=None,
    partition_columns=['jj_code', 'trade_date'],
    sort_columns=['jj_code', 'trade_date'],
    keep_duplicates=KeepDuplicate.FIRST
)

stock_pingji_table = PartitionTable(
    name="pingji_table",
    db_path=DfsDbPath.stock_daily_code,
    schema=None,
    partition_columns=['jj_code', 'trade_date'],
    sort_columns=['jj_code', 'trade_date'],
    keep_duplicates=KeepDuplicate.FIRST
)

if __name__ == '__main__':
    pass
