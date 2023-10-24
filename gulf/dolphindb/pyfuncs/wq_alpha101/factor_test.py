# !/usr/bin/env python3
# -*- coding:utf-8 -*-

# @Datetime : 2022/11/29 22:54
# @Author   : Fangyang
# @Software : PyCharm

import time

import pandas as pd
import polars as pl

from gulf.dolphindb.bond import BondDB
from gulf.dolphindb.pyfuncs.wq_alpha101 import *

table_name = "temp_t"
db = BondDB()


def read_table_test():
    start_time = time.perf_counter()
    table = db.get_bond_daily_table_df(
        table_name=table_name,
        start_date="2021.01.01",
        end_date="2022.10.15",
        fields=[
            "rsi(close, 14) as rsi",
            "(ratios(close)-1) as pct_chg1",
            "mavg(ratios(close), 3) as prev_avr3",
            "move(mavg(ratios(close), 3), -3) as next_avr3"
        ]
    )
    print(f"gen_stock_table cost {time.perf_counter() - start_time:.2f}s")


def alpha101_1to46_test():
    start_time = time.perf_counter()
    for i in range(1, 47):
        t1 = db.session.run(
            eval(f"wq_alpha{i}")(table_name)
        )
        print(t1.shape)
    print(f"compute alphas cost {time.perf_counter() - start_time:.2f}s")
    print(1)


def alpha101_47to62_test():
    start_time = time.perf_counter()
    factor_list = [47, 49, 50, 51, 52, 53, 54, 55, 57, 60, 61, 62]
    for i in factor_list:
        t1 = db.session.run(
            eval(f"wq_alpha{i}")(table_name)
        )
        print(t1.shape)
    print(f"compute alphas cost {time.perf_counter() - start_time:.2f}s")
    print(1)


def alpha101_topk_test():
    table: pd.DataFrame = db.get_bond_daily_table_df(table_name=table_name)
    start_time = time.perf_counter()
    db_t4 = db.session.run(
        wq_alpha4(table_name, alpha_name="alpha4", top_n=3)
    )
    print(f"Dolphindb groupby top k cost {time.perf_counter() - start_time:.2f}s")

    table: pd.DataFrame = db.get_bond_daily_table_df(table_name=table_name)
    table = db.session.run(
        wq_alpha4(table_name, alpha_name="alpha4")
    )
    start_time = time.perf_counter()
    pl_df = pl.from_pandas(table)
    print(f"Polars trans from pandas cost {time.perf_counter() - start_time:.2f}s")

    start_time = time.perf_counter()
    pl2_df = pl_df.to_pandas()
    print(f"Polars trans to pandas cost {time.perf_counter() - start_time:.2f}s")

    start_time = time.perf_counter()
    # pl_t4 = pl_df.groupby("trade_date").agg(
    #     [pl.col("jj_code"),
    #      pl.col("alpha4").reverse()]
    # ).sort(by='trade_date')
    pl_t4 = pl_df.sort(by='trade_date').select(
        pl.col("*").sort_by(pl.col("alpha4"), descending=True).head(3).list().over("trade_date").flatten(),
    )

    print(f"Polars groupby top k cost {time.perf_counter() - start_time:.2f}s")

    start_time = time.perf_counter()
    plpd_t4 = pl_t4.to_pandas()
    print(f"Polars trans to pandas cost {time.perf_counter() - start_time:.2f}s")

    start_time = time.perf_counter()
    pd_t4 = table.groupby('trade_date').apply(lambda x: x.nlargest(3, ['alpha4'])).reset_index(drop=True)
    print(f"Pandas groupby top k cost {time.perf_counter() - start_time:.2f}s")
    print(1)


if __name__ == '__main__':
    # read_table_test()
    # alpha101_topk_test()
    alpha101_1to46_test()
    alpha101_47to62_test()
