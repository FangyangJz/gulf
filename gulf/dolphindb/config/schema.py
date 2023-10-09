from typing import Dict, List

from gulf.dolphindb.const import DolType


class Schema:
    def __init__(self, columns_list, columns, types):
        self.columns_list = columns_list
        self.columns = columns
        self.types = types


def new_schema(
        cols_type_dict: Dict[str, DolType],
        columns_list: List[List[str]]
):
    columns = []
    types = []
    for c, t in cols_type_dict.items():
        columns.append(c)
        types.append(t.value)

    return Schema(
        columns_list=columns_list,
        columns=columns,
        types=types
    )


def index_daily_table_schema() -> Schema:
    cols_type_dict = {'jj_code': DolType.SYMBOL, }

    # tushare daily 数据不全, 这里使用akshare数据
    daily_cols_type_dict = {
        'trade_date': DolType.NANOTIMESTAMP,
        'open': DolType.DOUBLE, 'high': DolType.DOUBLE,
        'low': DolType.DOUBLE, 'close': DolType.DOUBLE,
        'pct_chg': DolType.DOUBLE, 'pct_amount': DolType.DOUBLE,
        'vol': DolType.DOUBLE, 'amount': DolType.DOUBLE,
        'hl_chg': DolType.DOUBLE, 'turnover_rate': DolType.DOUBLE,
    }

    cols_type_dict.update(daily_cols_type_dict)
    columns_list = [list(daily_cols_type_dict.keys())]

    return new_schema(cols_type_dict, columns_list)


def stock_daily_table_schema() -> Schema:
    cols_type_dict = {'jj_code': DolType.SYMBOL, }

    # tushare daily 数据不全, 这里使用akshare数据
    daily_cols_type_dict = {
        'ts_code': DolType.SYMBOL, 'trade_date': DolType.NANOTIMESTAMP,
        # 'pre_close': DolType.DOUBLE,
        'open': DolType.DOUBLE, 'high': DolType.DOUBLE,
        'low': DolType.DOUBLE, 'close': DolType.DOUBLE,
        # 'change': DolType.DOUBLE,
        'pct_chg': DolType.DOUBLE, 'volume': DolType.DOUBLE,
        'amount': DolType.DOUBLE,
        # 'adj_pre_close': DolType.DOUBLE,
        'adj_open': DolType.DOUBLE, 'adj_high': DolType.DOUBLE, 'adj_low': DolType.DOUBLE,
        'adj_close': DolType.DOUBLE,
        # 'adj_factor': DolType.DOUBLE, 'avg_price': DolType.DOUBLE,
        # 'trade_status': DolType.SYMBOL,
    }

    # 由于akshare的 pe pb ps 数据质量没有tushare好, 所以这里都用tushare数据
    daily_basic_ts_cols_type_dict = {
        'ts_code': DolType.SYMBOL, 'trade_date': DolType.NANOTIMESTAMP,
        # 'close': DolType.DOUBLE,
        'turnover_rate': DolType.DOUBLE,
        'turnover_rate_f': DolType.DOUBLE,
        'volume_ratio': DolType.DOUBLE,
        'pe': DolType.DOUBLE, 'pe_ttm': DolType.DOUBLE,
        'pb': DolType.DOUBLE,
        'ps': DolType.DOUBLE, 'ps_ttm': DolType.DOUBLE,
        'dv_ratio': DolType.DOUBLE, 'dv_ttm': DolType.DOUBLE,
        'total_share': DolType.DOUBLE,
        'float_share': DolType.DOUBLE,
        'free_share': DolType.DOUBLE,
        'total_mv': DolType.DOUBLE, 'circ_mv': DolType.DOUBLE,
        # 'udlimit_status': DolType.INT   # 有 nan, pandas 转 int 报错
    }
    custom_dict = {'name': DolType.SYMBOL, 'industry': DolType.SYMBOL}

    cols_type_dict.update(daily_cols_type_dict)
    cols_type_dict.update(daily_basic_ts_cols_type_dict)
    cols_type_dict.update(custom_dict)

    columns_list = [
        list(daily_cols_type_dict.keys()),
        list(daily_basic_ts_cols_type_dict.keys()),
        list(custom_dict.keys()),
    ]

    return new_schema(cols_type_dict, columns_list)


def stock_moneyflow_daily_table_schema() -> Schema:
    cols_type_dict = {}

    # tushare daily 数据不全, 这里使用akshare数据
    daily_cols_type_dict = {
        'in_amount_main': DolType.DOUBLE, 'in_ratio_main': DolType.DOUBLE,
        'in_amount_large': DolType.DOUBLE, 'in_ratio_large': DolType.DOUBLE,
        'in_amount_big': DolType.DOUBLE, 'in_ratio_big': DolType.DOUBLE,
        'in_amount_mid': DolType.DOUBLE, 'in_ratio_mid': DolType.DOUBLE,
        'in_amount_small': DolType.DOUBLE, 'in_ratio_small': DolType.DOUBLE,
        'jj_code': DolType.SYMBOL, 'name': DolType.SYMBOL, 'industry': DolType.SYMBOL,
        'trade_date': DolType.NANOTIMESTAMP,
    }

    cols_type_dict.update(daily_cols_type_dict)

    columns_list = [
        list(daily_cols_type_dict.keys()),
    ]

    return new_schema(cols_type_dict, columns_list)


def industry_moneyflow_daily_table_schema() -> Schema:
    cols_type_dict = {}

    # tushare daily 数据不全, 这里使用akshare数据
    daily_cols_type_dict = {
        'in_amount_main': DolType.DOUBLE, 'in_ratio_main': DolType.DOUBLE,
        'in_amount_large': DolType.DOUBLE, 'in_ratio_large': DolType.DOUBLE,
        'in_amount_big': DolType.DOUBLE, 'in_ratio_big': DolType.DOUBLE,
        'in_amount_mid': DolType.DOUBLE, 'in_ratio_mid': DolType.DOUBLE,
        'in_amount_small': DolType.DOUBLE, 'in_ratio_small': DolType.DOUBLE,
        'jj_code': DolType.SYMBOL,
        'trade_date': DolType.NANOTIMESTAMP,
    }

    cols_type_dict.update(daily_cols_type_dict)

    columns_list = [
        list(daily_cols_type_dict.keys()),
    ]

    return new_schema(cols_type_dict, columns_list)


def hk_hold_table_schema() -> Schema:
    """
    ['jj_code', 'ts_code', 'trade_date', 'name', 'vol', 'ratio', 'exchange']

    :return: None
    """

    hk_hold_cols_type_dict = {
        # 'code',
        'ts_code': DolType.SYMBOL, 'trade_date': DolType.NANOTIMESTAMP,
        'name': DolType.SYMBOL, 'vol': DolType.DOUBLE,
        'ratio': DolType.DOUBLE, 'exchange': DolType.SYMBOL
    }

    cols_type_dict = {'jj_code': DolType.SYMBOL, }
    cols_type_dict.update(hk_hold_cols_type_dict)

    columns_list = [list(hk_hold_cols_type_dict.keys())]

    return new_schema(cols_type_dict, columns_list)


def bond_daily_table_schema() -> Schema:
    """
    ['jj_code', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'bond_code',
   'bond_name', 'stock_code', 'stock_name', 'bond_scale', 'listing_date',
   'market', 'bond_value', 'trans_stock_value', 'bond_premium',
   'trans_stock_premium', 'duallow']

    :return: None
    """

    bond_daily_cols_type_dict = {
        'jj_code': DolType.SYMBOL, 'trade_date': DolType.NANOTIMESTAMP,
        'open': DolType.DOUBLE, 'high': DolType.DOUBLE, 'low': DolType.DOUBLE, 'close': DolType.DOUBLE,
        'volume': DolType.DOUBLE,
        'bond_code': DolType.SYMBOL, 'bond_name': DolType.SYMBOL,
        'stock_code': DolType.SYMBOL, 'stock_name': DolType.SYMBOL,
        'bond_scale': DolType.DOUBLE, 'listing_date': DolType.NANOTIMESTAMP,
        'market': DolType.SYMBOL,
        'bond_value': DolType.DOUBLE, 'trans_stock_value': DolType.DOUBLE, 'bond_premium': DolType.DOUBLE,
        'trans_stock_premium': DolType.DOUBLE, 'duallow': DolType.DOUBLE
    }

    cols_type_dict = dict()
    cols_type_dict.update(bond_daily_cols_type_dict)

    columns_list = [list(bond_daily_cols_type_dict.keys())]

    return new_schema(cols_type_dict, columns_list)


def crypto_depth_table_schema(ask_bid_nums: int = 20) -> Schema:
    """
    ['jj_code', 'trade_date', ...]

    :return: Schema
    """
    bid_price_col = {f"bp{i}": DolType.DOUBLE for i in range(ask_bid_nums)}
    bid_volume_col = {f"bv{i}": DolType.DOUBLE for i in range(ask_bid_nums)}
    ask_price_col = {f"ap{i}": DolType.DOUBLE for i in range(ask_bid_nums)}
    ask_volume_col = {f"av{i}": DolType.DOUBLE for i in range(ask_bid_nums)}

    crypto_order_book_cols_type_dict = {
        'jj_code': DolType.SYMBOL, 'trade_date': DolType.NANOTIMESTAMP,
    }

    cols_type_dict = dict()
    [cols_type_dict.update(e)
     for e in [crypto_order_book_cols_type_dict, bid_price_col, bid_volume_col, ask_price_col, ask_volume_col]
     ]
    columns_list = [list(crypto_order_book_cols_type_dict.keys())]

    return new_schema(cols_type_dict, columns_list)


def crypto_aggtrade_table_schema() -> Schema:
    """
    ['jj_code', 'trade_date', ...]

    :return: Schema
    """
    crypto_order_book_cols_type_dict = {
        'jj_code': DolType.SYMBOL, 'trade_date': DolType.NANOTIMESTAMP,
        'p': DolType.DOUBLE, 'q': DolType.DOUBLE,
        'trade_timestamp': DolType.NANOTIMESTAMP,
        'maker': DolType.INT
    }

    cols_type_dict = dict()
    cols_type_dict.update(crypto_order_book_cols_type_dict)
    columns_list = [list(crypto_order_book_cols_type_dict.keys())]

    return new_schema(cols_type_dict, columns_list)


if __name__ == '__main__':
    s = crypto_depth_table_schema()
    s1 = crypto_aggtrade_table_schema()
    print(1)
