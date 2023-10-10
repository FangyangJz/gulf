# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/10 8:29
# @Author   : Fangyang
# @Software : PyCharm

from gulf.dolphindb.db_path import DfsDbPath
from gulf.dolphindb.tables.partition.table import PartitionTable
from gulf.dolphindb.tables.partition.bond_schema import bond_daily_table_schema

bond_daily_table = PartitionTable(
    name="bond_daily_table",
    db_path=DfsDbPath.bond_daily_code,
    schema=bond_daily_table_schema()
)

if __name__ == '__main__':
    pass
