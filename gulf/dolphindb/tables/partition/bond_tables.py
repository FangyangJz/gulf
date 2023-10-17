# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/10 8:29
# @Author   : Fangyang
# @Software : PyCharm

from gulf.dolphindb.const import KeepDuplicate
from gulf.dolphindb.db_path import DfsDbPath
from gulf.dolphindb.tables.partition.table import PartitionTable
from gulf.dolphindb.tables.partition.bond_schema import bond_daily_table_schema

bond_daily_table = PartitionTable(
    name="bond_daily_table",
    db_path=DfsDbPath.bond_daily_code,
    schema=bond_daily_table_schema(),
    partition_columns=['jj_code', 'trade_date'],
    sort_columns=['jj_code', 'trade_date'],
    keep_duplicates=KeepDuplicate.FIRST
)

if __name__ == '__main__':
    pass
