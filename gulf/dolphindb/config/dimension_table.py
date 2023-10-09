import pandas as pd

from gulf.dolphindb.config.db_path import DfsDbPath


class DimensionTable:
    name: str = ""
    db_path: str = ""
    db_name: str = ""
    sort_columns: str = ""
    df: pd.DataFrame = None


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


class BondBasicTable(DimensionTable):
    name = "bond_basic_table"
    db_path = DfsDbPath.bond_basic
    db_name = "bond_basic_db"
    sort_columns = 'jj_code'

    def __init__(self):
        super().__init__()

        from avalon.datafeed.akshare.bond import gen_bond_basic_df

        table_df, _ = gen_bond_basic_df()
        table_df['上市时间'] = pd.to_datetime(table_df['上市时间'])
        table_df['转股起始日'] = pd.to_datetime(table_df['转股起始日'])

        self.df = table_df


class BondRedeemTable(DimensionTable):
    name = "bond_redeem_table"
    db_path = DfsDbPath.bond_basic
    db_name = "bond_basic_db"
    sort_columns = 'jj_code'

    def __init__(self):
        super().__init__()

        from avalon.datafeed.akshare.bond import gen_bond_basic_df

        _, redeem_table_df = gen_bond_basic_df()
        redeem_table_df['转股起始日'] = pd.to_datetime(redeem_table_df['转股起始日'])

        self.df = redeem_table_df


class BondDelistTable(DimensionTable):
    name = "bond_delist_table"
    db_path = DfsDbPath.bond_basic
    db_name = 'bond_basic_db'
    sort_columns = 'jj_code'

    def __init__(self):
        """
        117*** 是 EB可交换债, 正股代码 和 名称 为 None
        """
        super(BondDelistTable, self).__init__()

        from avalon.datafeed.xcsc_tushare import get_delist_bond_df

        table_df = get_delist_bond_df()
        table_df['trade_dt_last'] = pd.to_datetime(table_df['trade_dt_last'])
        self.df = table_df


class BondIndexTable(DimensionTable):
    name = "bond_index_table"
    db_path = DfsDbPath.bond_index
    db_name = 'bond_index_db'
    sort_columns = 'index'

    def __init__(self):
        super(BondIndexTable, self).__init__()

        from avalon.datafeed.akshare.bond import get_bond_index_daily

        table_df = get_bond_index_daily()
        table_df['trade_date'] = pd.to_datetime(table_df['trade_date'])
        table_df.reset_index(inplace=True)
        self.df = table_df


class TradeCalenderTable(DimensionTable):
    name = "trade_calender_table"
    db_path = DfsDbPath.trade_calender
    db_name = "trade_calender_db"
    sort_columns = 'index'

    def __init__(self):
        super().__init__()

        from avalon.datafeed.akshare.stock import get_trade_date_df

        df = get_trade_date_df().astype('str')
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
    # stock_basic_table = StockBasicTable()
    # bond_basic_table = BondBasicTable()
    bond_redeem_table = BondRedeemTable()
    # trade_calender_table = TradeCalenderTable()
    # cls_test = IndexHS300MembersTable
    # self_test = IndexHS300MembersTable()
    print(1)
