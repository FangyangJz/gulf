import time

import dolphindb as ddb
import dolphindb.settings as keys
import pandas as pd

from typing import Dict, Type, Union
from loguru import logger

from gulf.dolphindb.db_path import DfsDbPath
from gulf.dolphindb.const import DolType_2_np_dtype_dict, Engine
from gulf.dolphindb.tables.dimension.table import DimensionTable
from gulf.dolphindb.tables.partition.table import PartitionTable
from gulf.utils.network import get_lan_ip


class Dolphindb:

    def __init__(
            self, host: str = None, port: int = 8848,
            username: str = 'admin', password: str = '123456',
            engine: Engine = Engine.TSDB, enable_async: bool = False
    ):
        """
        :param host:
        :param port:
        :param username:
        :param password:
        """
        self.host = host if host else get_lan_ip()  # 这里注意clash不能使用TUN mode, 否则获取的ip地址不是局域网ip
        self.port = port
        logger.info(f"[Dolphindb] Connect to {self.host}:{self.port}")

        self.username = username
        self.password = password

        # enableASYNC=True , 默认值是 False
        # stream table 异步持久化需要这里设置为True, 否则报错
        # 但是 异步时 session.run() 会没有返回结果
        self.session = ddb.session(enableASYNC=enable_async)
        self.engine = engine

        self.session.connect(host=self.host, port=self.port, userid=self.username, password=self.password)

        # 创建连接池（用于数据写入）
        self.pool = ddb.DBConnectionPool(self.host, self.port, 1, self.username, self.password)

    @property
    def db_path_init_script_dict(self) -> Dict[str, str]:
        raise NotImplementedError

    def init_db(self, clear: bool = False):
        """
        :param clear: 默认为 False, 如果为 True, 强制清空数据库, 然后重新建立分区数据库
        """
        if clear:
            for db_path in self.db_path_init_script_dict.keys():
                if self.session.existsDatabase(db_path):
                    self.session.dropDatabase(db_path)
                    logger.info(f"[Dolphindb] Drop database: {db_path}")

        for db_path, script in self.db_path_init_script_dict.items():
            if not self.session.existsDatabase(db_path):
                self.session.run(script)

    def _create_table(
            self, table: PartitionTable,
            partition_sort_columns: str = "`jj_code`trade_date"
    ):
        """

        :param table:
        :param partition_sort_columns: 排序要把 时间写在最后, TSDB
        :return:
        """
        columns = table.schema.columns
        types = table.schema.types
        db_path = table.db_path
        table_name = table.name

        tsdb_args = f", sortColumns={partition_sort_columns}, keepDuplicates=FIRST" if self.engine == Engine.TSDB else ""
        create_table_script = f"""
                db=database('{db_path}')
                columns={columns}
                types={types}
                {table_name}=db.createPartitionedTable(table=table(100000:0,columns,types), tableName=`{table_name}, partitionColumns={partition_sort_columns}{tsdb_args})
                """  # 10_0000

        if not self.session.existsTable(dbUrl=db_path, tableName=table_name):
            self.session.run(create_table_script)
            logger.info(f"[Dolphindb] Create table db_path:{db_path}, table_name: {table_name}")
        else:
            logger.info(f"[Dolphindb] Table exist. No create, db_path:{db_path}, table_name: {table_name}")

    def get_dimension_table_df(
            self, dim_table_cls: Type[DimensionTable], from_db: bool = True
    ) -> Union[pd.DataFrame, None]:
        if from_db:
            try:
                df = self.session.loadTable(dim_table_cls.name, dim_table_cls.db_path).toDF()
                return df
            except Exception:
                logger.exception(f'Dimension table: {dim_table_cls.name}. DB path: {dim_table_cls.db_path}')
                return None
        else:
            table = dim_table_cls()
            self.save_dimension_table(dim_table=table)
            return table.df

    def save_dimension_table(self, dim_table: DimensionTable):
        """
        维度表, 用于存储元信息, 简短的表
        :param dim_table:
        :return:
        """

        db_path = dim_table.db_path
        table_name = dim_table.name

        if dim_table.df.empty:
            logger.error(f"[Dolphindb] Fail to save dimension table, {table_name} to {db_path}, table.df is empty")
            return

        if self.session.existsTable(dbUrl=db_path, tableName=table_name):
            self.session.dropTable(dbPath=db_path, tableName=table_name)

        db = self.session.database(
            dbName=dim_table.db_name,
            dbPath=db_path,
            partitionType=keys.VALUE,
            partitions=[1, 2, 3],
            engine=str(self.engine.value)
        )

        table = self.session.table(data=dim_table.df)

        # 创建维度表, 不用 createPartitionedTable
        db.createTable(
            table=table, tableName=table_name, sortColumns=dim_table.sort_columns
        )

        re = self.session.loadTable(tableName=table_name, dbPath=db_path)
        re.append(table=table)
        logger.success(f"[Dolphindb] Success save dimension table, {table_name} to {db_path}")

    def save_res_dict_to_db_table(
            self, partition_table: PartitionTable, res_dict: Dict, partition_col: str = 'jj_code'
    ):
        db_path = partition_table.db_path
        table_name = partition_table.name
        columns = partition_table.schema.columns
        types = partition_table.schema.types

        appender = ddb.PartitionedTableAppender(
            dbPath=db_path,
            tableName=table_name,
            partitionColName=partition_col,
            dbConnectionPool=self.pool
        )

        if not res_dict:
            return

        res_df = pd.concat(res_dict.values())
        res_df = res_df[columns]  # TODO 注意 !!! append 的 df columns 顺序必须和建表时的顺序一致

        # 取到的 volume 列数据类型不一致, 有int和float混合的问题, 导致写数据库的时候报错,
        # 这里做类型一致性检查和修正, 所以datetime类型在修正过程中直接转换了,省去下面这行
        # table_df['trade_date'] = pd.to_datetime(table_df['trade_date'])
        for col, dtype, dol_type in zip(columns, res_df.dtypes, types):
            if DolType_2_np_dtype_dict[dol_type] != dtype:
                res_df[col] = res_df[col].astype(DolType_2_np_dtype_dict[dol_type])

        start_time = time.perf_counter()
        appender.append(res_df)  # TODO 注意 !!! append 的 df columns 顺序必须和建表时的顺序一致
        logger.success(f"[Dolphindb] Write len:{len(res_df)} data to db, cost:{time.perf_counter() - start_time:.2f}s")

        res_dict.clear()

    def get_tables(self, db_path: str):
        return self.session.run(
            f"""
            db = database('{db_path}');
            getTables(db);
        """
        )

    def get_all_dfs_tables(self):
        r_dict = {}
        for dfs in self.db_path_init_script_dict.keys():
            r_dict[dfs] = self.get_tables(dfs)
        return r_dict

    def get_table_meta(self, db_path: str, table_name: str):
        return self.session.run(
            f"getTabletsMeta('/{db_path.split('://')[1]}/%', `{table_name}, true)"
        )

    def get_all_dfs_tables_meta(self):
        df_list = []
        for dfs, table_names in self.get_all_dfs_tables().items():
            if len(table_names) != 0:
                for table_name in table_names:
                    df_list.append(self.get_table_meta(db_path=dfs, table_name=table_name))
        return pd.concat(df_list)

    def delete_dfs_data_before_trade_date(self, db_path: DfsDbPath, table_name: str, trade_date: str):
        return self.session.run(
            f"""
                db = database('{db_path}');
                t = loadTable(db, `{table_name});
                delete from t where trade_date < {trade_date};
            """
        )


if __name__ == '__main__':
    db = Dolphindb(
        # host='192.168.0.104', port=8848,
        # username='admin', password='123456',
        # engine=Engine.TSDB,
        # clear_db=True
    )

    # df = db.get_all_dfs_tables_meta()
    # db.session.dropDatabase(BondBasicTable.db_path)
    # db.session.dropTable(db.stock_daily_code_db_path, "stock_daily_table")

    # 清空数据库数据
    # db.init_db(clear=True)

    # clear db
    # db.session.dropDatabase(dbPath=db.stock_daily_code_db_path)
    # db.session.dropTable(dbPath=db.stock_daily_code_db_path, tableName=table_name)
    # db.session.dropTable(dbPath=db.stock_daily_db_path, tableName="moneyflow_hsgt_table")

    # t = db.session.loadTable(tableName="moneyflow_hsgt_table", dbPath=db.stock_daily_db_path)
    # r1 = t.select("*").where("trade_date<=2022.09.01").sort(bys="trade_date").toDF()
    # r1.fillna(value=0, inplace=True)
    # r1['upper'], r1['mid'], r1['lower'] = adbband(r1['north_money'], length=15)
    # r1['entry'] = r1['north_money'] > r1['upper']
    # r1['exit1'] = r1['north_money'] < r1['mid']
    # r1['exit2'] = r1['north_money'] < r1['lower']
    # entry = r1.iloc[-1, :]['entry']

    # t = db.session.loadTable(tableName="stock_daily_table", dbPath=db.stock_daily_code_db_path)
    # r1 = t.select("*").where("jj_code='SHSE.600053'").toDF()  # .ols("high", ['low'])
    # r2 = t.select(['jj_code', "circ_mv"]).where("trade_date=2022.09.01").toDF()  # .ols("high", ['low'])
    # r = t.select(['trade_date', 'jj_code', 'low', 'high']).where("jj_code='SHSE.600053'").toDF()  # .ols("high", ['low'])
    # print(r1)

    # olss = t.select(['trade_date', 'jj_code', 'low', 'high']).where("jj_code='SHSE.600053'").ols("high", 'low')
    # df = db.trade_calender

    # t = db.session.loadTable(tableName="hk_hold_table", dbPath=db.stock_daily_code_db_path)
    # r3 = t.select("top 200 *")\
    #     .where("trade_date=2022.09.01, exchange!=`HK")\
    #     .sort(bys="ratio", ascending=False).toDF()  # .ols("high", ['low'])

    # r4 = t.select("top 200 *") \
    #     .where("jj_code='SZSE.002460', exchange!=`HK") \
    #     .sort(bys="ratio", ascending=False).toDF()  # .ols("high", ['low'])

    print(1)
