# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/10 6:37
# @Author   : Fangyang
# @Software : PyCharm

import datetime
from typing import Union, Dict, List

import pandas as pd
from tqdm import tqdm

from gulf.dolphindb.base import Dolphindb
from gulf.dolphindb.const import Engine
from gulf.dolphindb.db_path import DfsDbPath
from gulf.dolphindb.tables import TradeCalenderTable, StockBasicTable
from gulf.dolphindb.tables.partition.stock_tables import stock_nfq_daily_table


class StockDB(Dolphindb):
    # 从2000年开始, 有两个原因:
    # 1. 会计规则从1999年开始形成
    # 2. 样本数足够多
    # 从2005年开始, 沪深300指数从2005年1月4日起开始有完整数据
    start_year = 2005  # 用于 数据库分区 和 确定数据下载范围
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

        self.partition_tables = [stock_nfq_daily_table, ]
        [self._create_table(table=table) for table in self.partition_tables]

    @property
    def db_path_init_script_dict(self) -> Dict[str, str]:
        return {
            DfsDbPath.stock_daily_code: self.init_daily_code_db_script,
            DfsDbPath.stock_daily: self.init_daily_db_script,
        }

    @property
    def init_daily_code_db_script(self):
        return f"""
        db_code_hash=database(, HASH, [SYMBOL, 40])
        yearRange=date({self.start_year}.01M + 12*0..{self.end_year - self.start_year})
        db_date_range=database(, RANGE, yearRange)
        db=database('{DfsDbPath.stock_daily_code}', COMPO, [db_code_hash,db_date_range], engine=`{self.engine.value})
        """

    @property
    def init_daily_db_script(self):
        """
        只有日期 没有 股票代码
        """
        return f"""
        yearRange=date({self.start_year}.01M + 12*0..{self.end_year - self.start_year})
        db=database('{DfsDbPath.stock_daily}', RANGE, yearRange, engine=`{self.engine.value})
        """

    def update_stock_nfq_daily_table_by_reader(self, offset: int = 0):
        """
        读取本地数据, 写入db, mootdx reader 目前代码不涵盖 bj 路径, 故没有北交所数据
        :param offset: 大于等于0表示将全部数据写入db, -2 表示数据最近2天数据写入db
        :return:
        """
        from mootdx.reader import Reader

        reader = Reader.factory(market='std', tdxdir='C:/new_tdx')
        stock_basic_df = self.get_dimension_table_df(StockBasicTable, from_db=True)

        res_dict = dict()
        for idx, row in tqdm(stock_basic_df.iterrows()):
            stock_code = row['代码']

            stock_daily_df = reader.daily(symbol=stock_code)
            stock_daily_df = stock_daily_df if offset >= 0 else stock_daily_df.iloc[offset:]
            if stock_daily_df.empty:
                continue

            stock_daily_df['jj_code'] = row['jj_code']
            stock_daily_df['name'] = row['名称']
            stock_daily_df['industry'] = row['所处行业']

            res_dict[stock_code] = stock_daily_df

        df = pd.concat(res_dict.values()).reset_index(names=['trade_date'])
        res_dict.clear()
        self.save_res_dict_to_db_table(partition_table=stock_nfq_daily_table, res_dict={'df': df})

    def update_dimension_tables(self):
        trade_calender = self.get_dimension_table_df(TradeCalenderTable, from_db=False)
        stock_basic_df = self.get_dimension_table_df(StockBasicTable, from_db=False)

    def get_stock_daily_table_df(
            self, table_name: str = "temp_table",
            start_date: str = "2005.01.01",
            end_date: str = "",
            where_filter: str = "",
            market: str = "S",
            is_indclass_onehot: bool = False,
            end_sql: str = ""
    ) -> pd.DataFrame:
        """

        :param table_name:
        :param start_date:
        :param end_date:
        :param where_filter:
        :param market: 默认值为 'S' 表示排除北交所, 'SH' 表示沪市, 'SZ'表示深市, 'B' 表示北交所, '' 表示全部,
        :param is_indclass_onehot: 是否行业 onehot encode
        :param end_sql:
        :return:
        """

        dt = ""
        if start_date:
            dt += f"&& trade_date>={start_date} "
        if end_date:
            dt += f"&& trade_date<={end_date} "

        where_filter = "," + where_filter if where_filter else ""

        res = f"""
            use ta;
            db_path = "{stock_nfq_daily_table.db_path}";
            db = database(db_path);
            t = loadTable(db, "{stock_nfq_daily_table.name}");

            def get_raw_data_table(market='SH'){{
                return  select *, cumwavg(rowAvg(close, high, low), volume) as vwap  
                from t 
                where jj_code like concat(market, '%'),  
                jj_code not like '%.68%', jj_code not like '%.30%', name not like '%ST%'  
                {dt} {where_filter}  
                context by jj_code;
            }}

            {table_name} = get_raw_data_table(market='{market}');  // 排除北交所
            {f'{table_name} = oneHot({table_name}, `industry);' if is_indclass_onehot else ''}
            
            {end_sql}
            {table_name};
            """

        return self.session.run(res)


if __name__ == '__main__':
    db = StockDB()
    # db.update_dimension_tables()
    # db.update_stock_nfq_daily_table_by_reader(offset=-1)

    df0 = db.get_stock_daily_table_df(start_date='2023.10.10', is_indclass_onehot=True)
    df1 = db.get_stock_daily_table_df(start_date='2023.10.10', market='S')
    df2 = db.get_stock_daily_table_df(start_date='2023.10.10', market='')
    df3 = db.get_stock_daily_table_df(start_date='2023.10.10', market='B')
    df4 = db.get_stock_daily_table_df(start_date='2023.10.10', market='SH')
    df5 = db.get_stock_daily_table_df(start_date='2023.10.10', market='SZ')
    print(1)