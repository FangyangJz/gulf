# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/10 7:08
# @Author   : Fangyang
# @Software : PyCharm

import datetime

from typing import Union, Dict

from gulf.dolphindb.base import Dolphindb
from gulf.dolphindb.const import Engine
from gulf.dolphindb.db_path import DfsDbPath


class CryptoDB(Dolphindb):
    # 用于 数据库分区 和 确定数据下载范围
    start_year = 2022
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

        self.init_db(clear=clear_db)

    @property
    def db_path_init_script_dict(self) -> Dict[str, str]:
        return {
            DfsDbPath.crypto_lob: self.init_btc_lob_db_script,
        }

    @property
    def init_btc_lob_db_script(self):
        return f"""
        db_code_hash = database("", HASH,[SYMBOL,6])
        db_date_value = database(, VALUE, {self.start_year}.01.01..{self.end_year}.01.01)
        db=database('{DfsDbPath.crypto_lob}', COMPO, [db_date_value, db_code_hash], engine=`{self.engine.value})
        """


if __name__ == '__main__':
    pass
