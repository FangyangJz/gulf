# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/10 6:52
# @Author   : Fangyang
# @Software : PyCharm

import datetime
from typing import Union, Dict, List

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

        self.init_db(clear=clear_db)

        self.partition_tables = [bond_daily_table, ]
        [self._create_table(table=table) for table in self.partition_tables]

    @property
    def db_path_init_script_dict(self) -> Dict[str, str]:
        return {
            DfsDbPath.bond_daily_code: self.init_daily_code_db_script,
        }

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

    def update_bond_daily_table_by_akshare(self):
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

    def update_bond_daily_table_by_reader(self, offset: int = 0):
        """
        优点: 读取速度快, 交易中的债几乎不存在缺失问题
        缺点:
            1. 退市债没有数据, 转股溢价率是本地计算, 如果存在复权问题, 可能会存在问题, 需要后续确认,
            2. 没有纯债价值和纯债溢价率

        :param offset: 大于等于0表示将全部数据写入db, -2 表示数据最近2天数据写入db
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
            bond_daily_df = bond_daily_df if offset >= 0 else bond_daily_df.iloc[offset:]

            if bond_daily_df.empty:
                continue
            bond_daily_df[price_cols] = bond_daily_df[price_cols] / 10
            bond_daily_df['volume'] = bond_daily_df['volume'] * 1000

            stock_code = row['正股代码']
            trans_stock_price = row['转股价']

            # amount = price * volume * 100
            stock_daily_df = reader.daily(symbol=stock_code)
            stock_daily_df = stock_daily_df if offset >= 0 else stock_daily_df.iloc[offset:]
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

        df = pd.concat(res_dict.values()).reset_index().rename(columns={'date': 'trade_date'})
        res_dict.clear()
        self.save_res_dict_to_db_table(partition_table=bond_daily_table, res_dict={'df': df})

    def update_dimension_tables(self):
        bond_basic_df = self.get_dimension_table_df(BondBasicTable, from_db=False)
        bond_redeem_df = self.get_dimension_table_df(BondRedeemTable, from_db=False)

    def get_bond_daily_table_df(
            self, table_name: str = "temp_table",
            start_date: str = "2020.01.01",
            end_date: str = "",
            jj_code_list: List[str] = None,
            fields: List[str] = None,
            ctx_by_date_filters: List[str] = None,
            is_indclass_onehot: bool = False,
            end_sql: str = ""
    ) -> pd.DataFrame:
        """
        日常使用习惯方便的sql封装, 增加一些参数可以指定过滤
        1. 连表了StockBasicTable, 增加了行业, 股本, 每股净资产数据
        2. 行业信息可以通过参数指定是否需要onehot
        3. 可扩展的 end_sql

        :param table_name:
        :param start_date, "2005.01.01"
        :param end_date, 格式同上, 默认值为 "", 表示到数据库中最新的数据
        :param jj_code_list: ['SHSE.113635']
        :param fields: List[str], ['bond_scale', "stock_code", "trans_stock_premium"], 全部字段参考bond_daily_table.schema
        :param ctx_by_date_filters:
        :param is_indclass_onehot: 是否行业 onehot encode
        :param end_sql:
        :return: 返回 bond daily Dataframe
        """
        from gulf.dolphindb.tables.dimension.stock_tables import StockBasicTable

        dt = ""
        ctx_by_code_filters = []

        if start_date:
            dt += f"trade_date>={start_date} "
        if end_date:
            dt += f"&& trade_date<={end_date} "

        if dt:
            ctx_by_code_filters.append(dt)

        if jj_code_list:
            ctx_by_code_filters.append(f"jj_code in {jj_code_list}")
        # jj_code_filter = f"jj_code in {jj_code_list}" if jj_code_list else ""

        fields_str = "," + ",".join(fields) if fields else ""

        # filters = ", ".join([dt, jj_code_filter])
        ctx_by_code_filters_str = ", ".join(ctx_by_code_filters)

        if ctx_by_date_filters:
            filter_ctx_by_date = f"""
                    bond_table = select * from bond_table 
                    where {', '.join(ctx_by_date_filters)} 
                    context by trade_date;
                """
        else:
            filter_ctx_by_date = ""

        res = f"""
                use ta;
                db_path = "{bond_daily_table.db_path}";
                db = database(db_path);
                t = loadTable(db, "{bond_daily_table.name}");

                bond_table = select jj_code, bond_name, stock_name, trade_date, stock_code as securityid,
                open, high, low, close, volatile=high-low, volume, rowAvg(close, high, low, open) * volume as amount, 
                (ratios(close)-1)*100 as pct_chg, cumwavg(rowAvg(close, high, low), volume) as vwap,  
                bond_scale, trans_stock_premium, duallow {fields_str}  
                from t 
                where {ctx_by_code_filters_str} 
                context by jj_code;

                {filter_ctx_by_date}

                table6_name = "{StockBasicTable.name}";
                db6_path = "{StockBasicTable.db_path}";
                db6 = database(db6_path);
                table6 = loadTable(db6, table6_name);
                // alpha101 infoData , 没有 cap 总市值, 只有行业信息, 因为cap只与 alpha56 有关

                infoData = select 
                代码 as securityid, 所处行业 as indclass, 
                已流通股份 as stock_cshare, 总股本 as stock_tshare, 每股净资产 as stock_nav  
                from table6 where 所处行业!=NULL;

                {f'infoData = oneHot(infoData, `indclass);' if is_indclass_onehot else ''}

                {table_name} = select * from ej(bond_table, infoData, `securityid) order by jj_code, trade_date;

                {end_sql}

                {table_name};
                """

        return self.session.run(res)


if __name__ == '__main__':
    db = BondDB()

    # db.update_dimension_tables()
    # bond_basic_df = db.get_dimension_table_df(BondBasicTable, from_db=True)

    # Note: BondBasicTable 从网上获取的全部转债, 
    # 如果包含退市债, 历史日数据 47 万
    # 如果不包含退市债, 历史日数据 24 万
    # sina 网络的接口有限速, 当前配置可能需要调整
    # db.update_bond_daily_table_by_akshare()

    # db.update_bond_daily_table_by_reader(offset=-1)

    df = db.get_bond_daily_table_df(start_date='2023.10.10', is_indclass_onehot=True)
    print(1)
