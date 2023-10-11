# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/10 6:52
# @Author   : Fangyang
# @Software : PyCharm

import datetime
from typing import Union

import numpy as np
import pandas as pd
from tqdm import tqdm

from gulf.dolphindb.base import Dolphindb
from gulf.dolphindb.const import Engine
from gulf.dolphindb.db_path import DfsDbPath
from gulf.dolphindb.tables import bond_daily_table
from gulf.dolphindb.tables.dimension.bond_tables import BondBasicTable, BondRedeemTable


class BondDB(Dolphindb):
    # 用于 数据库分区 和 确定数据下载范围, TDX 可转债指数从 2020年5月14日 开始
    start_year = 2020
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
        """
        优点: 数据全面, 包含退市债可以达到 48w 数据量, 有纯债价值和纯债溢价率, 但是意义不大在 schema 中注释掉
        缺点:
            1. 下载速度慢, 大概需要5分钟左右,
            2. 下载中途可能因为 sina 限速导致部分数据无法下载成功
        """
        from gulf.akshare.bond import update_bond_daily_res_dict_thread

        res_dict = dict()
        update_bond_daily_res_dict_thread(
            bond_basic_df=self.get_dimension_table_df(BondBasicTable, from_db=True),
            res_dict=res_dict
        )

        self.save_res_dict_to_db_table(partition_table=bond_daily_table, res_dict=res_dict)

    def read_bond_daily_table(self):
        """
        优点: 读取速度快, 交易中的债几乎不存在缺失问题
        缺点:
            1. 退市债没有数据, 转股溢价率是本地计算, 如果存在复权问题, 可能会存在问题, 需要后续确认,
            2. 没有纯债价值和纯债溢价率
        """
        from mootdx.reader import Reader

        reader = Reader.factory(market='std', tdxdir='C:/new_tdx')
        bond_basic_df = self.get_dimension_table_df(BondBasicTable, from_db=True)

        res_dict = dict()
        for idx, row in tqdm(bond_basic_df.iterrows()):
            bond_code = row['债券代码']

            # price/10, volume*1000, amount keep
            price_cols = ['open', 'high', 'low', 'close']
            bond_daily_df = reader.daily(symbol=bond_code)
            if bond_daily_df.empty:
                continue
            bond_daily_df[price_cols] = bond_daily_df[price_cols] / 10
            bond_daily_df['volume'] = bond_daily_df['volume'] * 1000

            stock_code = row['正股代码']
            trans_stock_price = row['转股价']

            # amount = price * volume * 100
            stock_daily_df = reader.daily(symbol=stock_code)
            stock_daily_df.columns = [f'stock_{c}' for c in stock_daily_df.columns]

            # 转股溢价率=可转债价格/转股价值-1，转股价值=可转债面值/转股价*正股价。
            dd = pd.concat([bond_daily_df, stock_daily_df], axis=1).dropna()
            dd['trans_stock_value'] = 100 / trans_stock_price * stock_daily_df['stock_close']  # 转股价值
            dd['trans_stock_premium'] = dd['close'] / dd['trans_stock_value'] - 1  # 转股溢价率
            # dd['bond_value'] =  # 纯债价值
            # dd['bond_premium'] =  # 纯债溢价率
            dd['duallow'] = dd['close'] + dd['trans_stock_premium']
            dd['jj_code'] = row['jj_code']
            dd['bond_scale'] = row['发行规模'] if np.isnan(row['剩余规模']) else row['剩余规模']
            dd['listing_date'] = pd.to_datetime(row['上市时间'])
            dd['stock_name'] = row['正股简称']
            dd['bond_name'] = row['债券简称']
            dd['market'] = row['market']
            dd['bond_code'] = bond_code
            dd['stock_code'] = stock_code

            res_dict[bond_code] = dd

        df = pd.concat(res_dict.values()).reset_index(names=['trade_date'])
        res_dict.clear()
        self.save_res_dict_to_db_table(partition_table=bond_daily_table, res_dict={'df': df})

    def update_dimension_tables(self):
        bond_basic_df = self.get_dimension_table_df(BondBasicTable, from_db=False)
        bond_redeem_df = self.get_dimension_table_df(BondRedeemTable, from_db=False)


if __name__ == '__main__':
    db = BondDB()

    db.update_dimension_tables()
    bond_basic_df = db.get_dimension_table_df(BondBasicTable, from_db=True)

    # Note: BondBasicTable 从网上获取的全部转债, 
    # 如果包含退市债, 历史日数据 47 万
    # 如果不包含退市债, 历史日数据 24 万
    # sina 网络的接口有限速, 当前配置可能需要调整
    db.download_bond_daily_table()

    # db.read_bond_daily_table()
