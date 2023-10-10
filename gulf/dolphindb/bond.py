# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/10 6:52
# @Author   : Fangyang
# @Software : PyCharm

import datetime

from typing import Union

from gulf.dolphindb.base import Dolphindb
from gulf.dolphindb.const import Engine
from gulf.dolphindb.db_path import DfsDbPath
from gulf.dolphindb.tables import bond_daily_table


class BondDB(Dolphindb):
    start_year = 2008  # 用于 数据库分区 和 确定数据下载范围
    end_year = 2030

    def __init__(
            self, host: Union[str, None] = None, port: int = 8848,
            username: str = 'admin', password: str = '123456',
            engine: Engine = Engine.TSDB, clear_db: bool = False,
            enable_async: bool = False
    ):
        super().__init__(
            host=host, port=port, username=username, password=password,
            engine=engine, enable_async=enable_async
        )
        self.start_datetime = datetime.datetime(year=self.start_year, month=1, day=1)
        self.init_db_path_dict = {
            DfsDbPath.bond_daily_code: self.init_daily_code_db_script,
        }

        self.init_db(init_db_path_dict=self.init_db_path_dict, clear=clear_db)

        self.partition_tables = [bond_daily_table]
        [self._create_table(table=table) for table in self.partition_tables]

    @property
    def init_daily_code_db_script(self):
        """
        注意! 由于column指定了其他, 不是默认值, [db_code_hash,db_date_range] 修改过顺序
        """

        return f"""
        db_code_hash=database(, HASH, [SYMBOL, 6])
        yearRange=date({self.start_year}.01M + 12*0..{self.end_year - self.start_year})
        db_date_range=database(, RANGE, yearRange)
        db=database('{DfsDbPath.bond_daily_code}', COMPO, [db_code_hash,db_date_range], engine=`{self.engine.value})
        """

    def download_bond_daily_table(self):
        from gulf.akshare.bond import update_bond_daily_res_dict_thread
        from gulf.dolphindb.tables.dimension.bond_tables import BondBasicTable

        res_dict = dict()
        update_bond_daily_res_dict_thread(
            bond_basic_df=self.get_dimension_table_df(BondBasicTable, from_db=False),
            res_dict=res_dict
        )
        self.save_res_dict_to_table(partition_table=bond_daily_table, res_dict=res_dict)


if __name__ == '__main__':
    db = BondDB()

    # Note: BondBasicTable 从网上获取的全部转债, 
    # 如果包含退市债, 历史日数据 47 万
    # 如果不包含退市债, 历史日数据 24 万
    # sina 网络的接口有限速, 当前配置可能需要调整
    db.download_bond_daily_table()
