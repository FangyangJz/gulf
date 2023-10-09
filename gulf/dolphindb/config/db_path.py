dfs_header = "dfs://"  # 不能有 sub 文件夹


class DfsDbPath:
    trade_calender = dfs_header + "trade_calender"

    index_daily_code = dfs_header + "index_daily_code"

    stock_basic = dfs_header + "stock_basic"
    stock_index = dfs_header + "stock_index"
    stock_daily_code = dfs_header + "stock_daily_code"
    stock_daily = dfs_header + "stock_daily"  # 资金流

    bond_basic = dfs_header + "bond_basic"
    bond_index = dfs_header + "bond_index"
    bond_daily_code = dfs_header + "bond_daily_code"

    btc_lob = dfs_header + "btc_lob"


if __name__ == '__main__':
    pass
