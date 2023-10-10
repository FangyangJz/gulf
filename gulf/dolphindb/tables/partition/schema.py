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
