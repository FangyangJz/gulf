# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/3/5 21:06
# @Author   : Fangyang
# @Software : PyCharm
from tqdm import tqdm
import time
import numpy as np
import pandas as pd
from avalon.dolphindb import Dolphindb
from avalon.dolphindb.config.stream_table import StreamTable
from avalon.dolphindb.config.schema import crypto_depth_table_schema

table_name = "btc_table"

if __name__ == '__main__':
    db = Dolphindb()

    # pub
    t = StreamTable(name=table_name, schema=crypto_depth_table_schema(1))

    try:
        db.session.run(t.create_table())
    except Exception as e:
        print(e)

    for i in tqdm(range(10000)):
        # 注意！column 的顺序要和 stream schema 一致，否则grafana不显示数据
        df = pd.DataFrame(
            [[
                "btc",
                np.datetime64("now"),
                np.random.randn(),
                np.random.randn(),
                np.random.randn(),
                np.random.randn(),
            ]],
            columns=["jj_code", 'trade_date', "bp0", "bv0", 'ap0', "av0"]
        )
        db.session.run(t.update_table(), df)
        print(df)
        # 如果 dolphindb 的 session(enableASYNC=True)，session.run()
        print(f"[Dolphindb] Write stream table {db.session.run(t.get_all_persist_table())}")
        print(f"[Dolphindb] Write stream table {db.session.run(t.get_persist_info())}")
        time.sleep(1)
