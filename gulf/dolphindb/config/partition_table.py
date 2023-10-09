from typing import Union

from gulf.dolphindb.config.db_path import DfsDbPath
from gulf.dolphindb.config.schema import (
    stock_daily_table_schema, hk_hold_table_schema, bond_daily_table_schema,
    Schema, index_daily_table_schema, stock_moneyflow_daily_table_schema, industry_moneyflow_daily_table_schema
)


class PartitionTable:
    def __init__(self, name: str, db_path: str, schema: Union[Schema, None]):
        self.name = name
        self.db_path = db_path
        self.schema = schema


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

bond_daily_table = PartitionTable(
    name="bond_daily_table",
    db_path=DfsDbPath.bond_daily_code,
    schema=bond_daily_table_schema()
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
