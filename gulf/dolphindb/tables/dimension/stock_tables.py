# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/10 9:39
# @Author   : Fangyang
# @Software : PyCharm

import pandas as pd

from gulf.dolphindb.db_path import DfsDbPath
from gulf.dolphindb.tables.dimension.table import DimensionTable


class StockBasicTable(DimensionTable):
    name = "stock_basic_table"
    db_path = DfsDbPath.stock_basic
    db_name = "stock_basic_db"
    sort_columns = 'jj_code'

    def __init__(self):
        super().__init__()

        from gulf.akshare.stock import get_all_stocks_df

        table_df = get_all_stocks_df()
        # table_df['list_date'] = pd.to_datetime(table_df['list_date'])
        # table_df['delist_date'] = pd.to_datetime(table_df['delist_date'])

        self.df = table_df


class TradeCalenderTable(DimensionTable):
    name = "trade_calender_table"
    db_path = DfsDbPath.trade_calender
    db_name = "trade_calender_db"
    sort_columns = 'index'

    def __init__(self):
        super().__init__()

        import akshare as ak

        df = ak.tool_trade_date_hist_sina().astype('str')
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df.reset_index(inplace=True)
        self.df = df


class IndexHS300MembersTable(DimensionTable):
    name = "index_hushen300_members_table"
    db_path = DfsDbPath.stock_index
    db_name = "stock_index_db"
    sort_columns = "jj_code"

    def __init__(self):
        """
        ['jj_code', 'ts_code', 'con_ts_code', 'in_date', 'out_date', 'cur_sign']

        名称	        类型	    默认显示	描述
        ts_code 	str	    Y	    ts代码
        con_ts_code	str	    Y	    成份股ts代码
        in_date	    str	    Y	    纳入日期
        out_date	str	    Y	    剔除日期
        cur_sign	float	Y	    最新标志
        """
        super().__init__()

        from avalon.datafeed.xcsc_tushare import get_hushen300_index_members_df, trans_tushare_code_to_juejin_code

        table_df = get_hushen300_index_members_df()
        table_df['jj_code'] = trans_tushare_code_to_juejin_code(table_df['con_ts_code'])
        table_df['in_date'] = pd.to_datetime(table_df['in_date'])
        table_df['out_date'] = pd.to_datetime(table_df['out_date'])
        self.df = table_df


class IndexHS300DailyTable(DimensionTable):
    name = "index_hushen300_members_table"
    db_path = DfsDbPath.stock_index
    db_name = "stock_index_db"
    sort_columns = 'index'

    def __init__(self):
        super().__init__()
        from avalon.datafeed.xcsc_tushare import trans_tushare_code_to_juejin_code
        from avalon.datafeed.xcsc_tushare.stock import get_hushen300_index_daily_df

        df = get_hushen300_index_daily_df()
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df['jj_code'] = trans_tushare_code_to_juejin_code(df['ts_code'])
        df.reset_index(inplace=True)

        self.df = df


if __name__ == '__main__':
    pass
