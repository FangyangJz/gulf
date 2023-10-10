from typing import Union
from gulf.dolphindb.tables.partition.schema import Schema, crypto_depth_table_schema


class StreamTable:
    def __init__(
            self, name: str, schema: Union[Schema, None],
            length: int = 10000, key: str = "`jj_code`trade_date",
            is_persist: bool = True, cache_size: int = None, pre_cache: int = None
    ):
        """

        :param name:
        :param schema: 默认 crypto_order_book_table_schema(20)， 创建上下20档 order book
        :param length:
        :param key:
        :param is_persist: 是否持久化
        :param cache_size: 默认为10倍流表长度， 流表数据量达到 cache_size 行时持久化
        :param pre_cache: 默认为5倍流表长度， 持久化时，pre_cache 行进行异步方式压缩保存

        若执行enableTableShareAndPersistence时，
        磁盘上已经存在sharedPubTable表的持久化数据，那么系统会加载最新的preCache行记录到内存中。

        对于持久化是否启用异步，需要在持久化数据一致性和性能之间作权衡。当流数据的一致性要求较高时，
        可以使用同步方式，这样可以保证持久化完成以后，数据才会进入发布队列；
        若对实时性要求较高，不希望磁盘IO影响到流数据的实时性，则可启用异步方式。
        只有启用异步方式时，持久化工作线程数persistenceWorkerNum配置项才会起作用。
        若有多个发布表需要持久化，增加persistenceWorkerNum的配置值可以提升异步保存的效率。
        """

        assert length > 0
        assert isinstance(schema, Schema)

        self.name = name
        self.schema = schema
        self.length = length
        self.key = key
        self.is_persist = is_persist
        self.cache_size = cache_size if cache_size else length * 10
        self.pre_cache = pre_cache if pre_cache else length * 5

    def create_table(self):
        """
        https://dolphindb.cn/cn/help/FunctionsandCommands/CommandsReferences/e/enableTableShareAndPersistence.html?highlight=enabletableshareand

        enableTableShareAndPersistence(
        table, tableName, [asynWrite=true], [compress=true], [cacheSize=-1],
        [retentionMinutes=1440], [flushMode=0], [preCache])
        其中的retentionMinutes参数可以控制自动清除过期数据的时间，是一个整数，表示log文件的保留时间（从文件的最后修改时间开始计算），
        单位是分钟。默认值是1440，即一天。
        上面提问中的代码没有设置retentionMinutes的值，就是默认的1440，即一天。在节点正常运行的情况下，会自动清除磁盘上24h前的数据。
        :return:
        """
        s = f"""
            t = keyedStreamTable({self.key}, {self.length}:0,  {self.schema.columns}, [{",".join(self.schema.types)}])
        """
        if self.is_persist:
            persist_s = f"""
            enableTableShareAndPersistence(table=t, tableName=`{self.name}, cacheSize={self.cache_size}, preCache={self.pre_cache})
            """
        else:
            persist_s = f"""
            share t as {self.name}
            """

        script = (
            f"{s}"
            f"{persist_s}"
        )

        return script

    def create_ts_agg_table(
            self,
            table_name: Union[str, None] = None,
            window_size: int = 300,
            step: int = 30,
            metrics: Union[str, None] = None,
            dummy_table: Union[str, None] = None,
            output_table: Union[str, None] = None,
            time_column: str = "`trade_date",
            key_column: str = "`jj_code`trade_date"
    ):
        """
        通过以下方式订阅
        subscribeTable(tableName="Trade", actionName="act_tsaggr", offset=0, handler=append!{tsAggrKline}, msgAsTable=true);

        :param table_name:
        :param window_size:
        :param step:
        :param metrics: <[first(Price), max(Price),min(Price),last(Price),sum(volume)]>
        :param dummy_table:
        :param output_table:
        :param time_column:
        :param key_column:
        :return:
        """

        table_name = table_name if table_name else f"{self.name}_ts_agg"

        return f"""
            {table_name} = createTimeSeriesAggregator(
                name={table_name}, 
                windowSize={window_size}, 
                step={step}, 
                metrics={metrics}, 
                dummyTable={dummy_table}, 
                outputTable={output_table}, 
                timeColumn={time_column}, 
                keyColumn={key_column}
            );
        """

    def clear_persist(self):
        return f"""
            clearTablePersistence({self.name})
        """

    def close_persist(self):
        return f"""
            disableTablePersistence({self.name})
        """

    def get_persist_info(self):
        return f"""
            getPersistenceMeta({self.name})
        """

    @staticmethod
    def get_all_persist_table():
        """
        db.session.run(table.get_all_persist_table())
        返回全部持久化 stream table 的 table_name list
        :return:
        """
        return f"""
        exec filename from files(getConfigure("persistenceDir")+"/") where filename != "persistOffset" 
        """

    @staticmethod
    def cancel_all_subscribe():
        return f"""
            t = getStreamingStat().pubTables
            for(row in t){{
                tableName = row.tableName
                actions = split(substr(row.actions, 1, strlen(row.actions)-2), ",")
                for(action in actions){{
                    unsubscribeTable(tableName=tableName, actionName=action)
                }}
            }}
        """

    def drop_table(self):
        """
        删除内存和磁盘上的表, 需要在没有订阅的时候才能删除, 否则报错
        """
        return f"""
            {self.cancel_all_subscribe()}
            dropStreamTable(`{self.name})
        """

    def update_table(self):
        """
        注意！！这里一定不能换行！！！！
        :return:
        """
        return f"""tableInsert{{{self.name}, }}"""


if __name__ == '__main__':
    import time
    import numpy as np
    import pandas as pd
    from gulf.dolphindb.crypto import CryptoDB

    db = CryptoDB()
    table_name = "btc_table"

    # pub
    t = StreamTable(name=table_name, schema=crypto_depth_table_schema(1))
    db.session.run(t.create_table())
    for i in range(5):
        df = pd.DataFrame(
            [[
                np.datetime64("now"),
                "btc",
                np.random.randn(),
                np.random.randn(),
                np.random.randn(),
                np.random.randn(),
            ]],
            columns=['trade_date', "jj_code", 'ap0', "av0", "bp0", "bv0"]
        )
        time.sleep(1)
        db.session.run(t.update_table(), df)


