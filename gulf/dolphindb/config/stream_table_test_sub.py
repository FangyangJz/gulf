# !/usr/bin/env python3
# -*- coding:utf-8 -*-

# @Datetime : 2023/3/5 21:06
# @Author   : Fangyang
# @Software : PyCharm

import pandas as pd


def test_handler(els):
    global test_df
    test_df = pd.concat([test_df, pd.Series([els])])
    print(test_df.shape)
    print(els)


if __name__ == '__main__':
    from gulf.dolphindb import Dolphindb
    from threading import Event
    from stream_table_test_pub import table_name
    from avalon.dolphindb.config.stream_table import StreamTable

    db = Dolphindb()

    db.session.enableStreaming()

    if db.session.run(StreamTable.get_all_persist_table()) is not None:
        test_df = db.session.run(f"select * from {table_name}")
        db.session.subscribe(
            db.host, 8848, handler=test_handler, tableName=table_name,
            # offset=100
        )
        Event().wait()
    else:
        print("None persist_table or dolphindb session is ASYNC=True.")

    # db.session.run(t.drop_table())
