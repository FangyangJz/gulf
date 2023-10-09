# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/9 21:29
# @Author   : Fangyang
# @Software : PyCharm


from typing import Union, List, Any

# 市场
MARKET_SZ = 0  # 深市
MARKET_SH = 1  # 沪市
MARKET_BJ = 2  # 北交


def get_stock_markets(symbols=None) -> List[List[Union[Union[int, str], Any]]]:
    results = []

    assert isinstance(symbols, list), 'stock code need list type'

    if isinstance(symbols, list):
        for symbol in symbols:
            results.append([get_stock_market(symbol, string=False), symbol.strip('sh').strip('sz')])

    return results


def get_stock_market(symbol: str = '', string: bool = False) -> Union[int, str]:
    """ 判断股票ID对应的证券市场匹配规则

    ['50', '51', '60', '90', '110'] 为 sh
    ['00', '12'，'13', '18', '15', '16', '18', '20', '30', '39', '115'] 为 sz
    ['5', '6', '9'] 开头的为 sh， 其余为 sz

    :param string: False 返回市场ID，否则市场缩写名称
    :param symbol: 股票ID, 若以 'sz', 'sh' 开头直接返回对应类型，否则使用内置规则判断
    :return 'sh' or 'sz'
    """

    assert isinstance(symbol, str), 'stock code need str type'

    market = 'sh'

    if symbol.startswith(('sh', 'sz', 'SH', 'SZ')):
        market = symbol[:2].lower()

    elif symbol.startswith(('50', '51', '60', '68', '90', '110', '113', '132', '204')):
        market = 'sh'

    elif symbol.startswith(('00', '12', '13', '18', '15', '16', '18', '20', '30', '39', '115', '1318')):
        market = 'sz'

    elif symbol.startswith(('5', '6', '9', '7')):
        market = 'sh'

    elif symbol.startswith(('4', '8')):
        market = 'bj'

    if string is False:

        if market == 'sh':
            market = MARKET_SH

        if market == 'sz':
            market = MARKET_SZ

        if market == 'bj':
            market = MARKET_BJ

    return market


if __name__ == '__main__':
    pass
