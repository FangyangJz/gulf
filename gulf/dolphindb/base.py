import datetime
import threading
import time

import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd

from tqdm import tqdm
from typing import List, Dict, Callable, Type

from gulf.dolphindb.db_path import DfsDbPath
from gulf.dolphindb.const import DolType_2_np_dtype_dict, Engine
from gulf.dolphindb.tables.dimension.table import DimensionTable
from gulf.dolphindb.tables.partition.stock_tables import index_concept_daily_table
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
        :param clear_db: 注意!! 设置为True会情况所有数据, 一定要慎用!
        """
        self.host = host if host else get_lan_ip()  # 这里注意clash不能使用TUN mode, 否则获取的ip地址不是局域网ip
        self.port = port
        print(f"[Dolphindb] Connect to {self.host}:{self.port}")

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

        # self.partition_tables = [
        #     stock_daily_table, stock_moneyflow_daily_table, industry_moneyflow_daily_table,
        #     hk_hold_table, bond_daily_table,
        #     index_concept_daily_table, index_industry_daily_table
        # ]
        # [self._create_table(table=table) for table in self.partition_tables]

    def init_db(self, init_db_path_dict: Dict[str, str], clear: bool = False):
        """
        :param init_db_path_dict:
        :param clear: 默认为 False, 如果为 True, 强制清空数据库, 然后重新建立分区数据库
        """
        if clear:
            for db_path in init_db_path_dict.keys():
                if self.session.existsDatabase(db_path):
                    self.session.dropDatabase(db_path)
                    print(f"[Dolphindb] Drop database: {db_path}")

        for db_path, script in init_db_path_dict.items():
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
            print(f"[Dolphindb] Create table db_path:{db_path}, table_name: {table_name}")
        else:
            print(f"[Dolphindb] Table exist. No create, db_path:{db_path}, table_name: {table_name}")

    def get_dimension_table_df(self, dim_table_cls: Type[DimensionTable], from_db: bool = True):
        if from_db:
            try:
                df = self.session.loadTable(dim_table_cls.name, dim_table_cls.db_path).toDF()
                return df
            except Exception as e:
                print(e)
                return None
        else:
            table = dim_table_cls()
            self.save_dimension_table(dim_table=table)
            return table.df

    def download_stock_daily_code_by_code(
            self, table: PartitionTable,
            start_idx: int = -1,
            end_idx: int = 10_0000,
            start_date: str = None,
            end_date: str = "20500101",
            specify_code_list: List[str] = None
    ):
        from avalon.datafeed.mix.stock import update_stock_daily_code_res_dict_by_code_thread
        from avalon.datafeed.akshare.stock import AKSHARE_CRAWL_CONCURRENT_LIMIT, AKSHARE_CRAWL_STOP_INTERVEL
        from avalon.datafeed.xcsc_tushare import TUSHARE_CRAWL_CONCURRENT_LIMIT, TUSHARE_CRAWL_STOP_INTERVEL

        if start_date is None:
            start_date = f"{self.stock_start_year}0101"

        db_path = table.db_path
        table_name = table.name
        columns_list = table.schema.columns_list
        columns = table.schema.columns
        types = table.schema.types

        appender = ddb.PartitionedTableAppender(
            dbPath=db_path,
            tableName=table_name,
            partitionColName='jj_code',
            dbConnectionPool=self.pool
        )

        ts_code_df = self.get_dimension_table_df(StockBasicTable, from_db=False)
        if specify_code_list:
            ts_code_df = ts_code_df[ts_code_df['ts_code'].isin(specify_code_list)]

        print("\n" + "=" * 50)
        print(f"[Dolphindb] Iter Code. Start to download [{table_name}] data, db path:{db_path}")
        print(f"[Dolphindb] Iter Code. Code length {len(ts_code_df)}")
        print(f"[Dolphindb] Iter Code. Code idx range [{start_idx}:{end_idx}]")
        print(f"[Dolphindb] Iter Code. Date range [{start_date}:{end_date}]")
        print(f"[Dolphindb] Iter Code. Specify_code_list: {specify_code_list}")
        print("=" * 50)

        t_list = []
        res_dict = dict()
        error_code_list = []
        for idx, ss in tqdm(ts_code_df.iterrows(), desc=f"Downloading {table_name} by iter code"):

            if idx < start_idx:
                continue
            elif idx >= end_idx:
                break

            name = ss['名称']
            ts_code = ss['ts_code']
            industry = ss['所处行业']

            # update_stock_daily_code_res_dict_by_code_thread(
            #     code, res_dict, columns_list, error_code_list, start_date, end_date
            # )

            t = threading.Thread(
                target=update_stock_daily_code_res_dict_by_code_thread,
                args=(ts_code, name, industry, res_dict, columns_list, error_code_list, start_date, end_date)
            )
            t.start()
            t_list.append(t)

            ##### for debug
            # if idx == 10:
            #     break

            # 遵守 akshare 访问限制, test 下来好像不好用, 所以用分段下载的方式下载
            if len(t_list) > AKSHARE_CRAWL_CONCURRENT_LIMIT:
                [t.join() for t in t_list]
                t_list = []

                # res_dict 中没有数据, 跳出循环
                if not res_dict:
                    break

                self.save_res_dict_df_to_db(appender=appender, res_dict=res_dict, columns=columns, types=types)

                print(f"[Dolphindb] In case of ban ip, wait {AKSHARE_CRAWL_STOP_INTERVEL}s ...")
                time.sleep(AKSHARE_CRAWL_STOP_INTERVEL)

            # 遵守 tushare 40次/秒 访问接口的限制
            elif len(t_list) > TUSHARE_CRAWL_CONCURRENT_LIMIT:
                [t.join() for t in t_list]
                t_list = []

                # res_dict 中没有数据, 跳出循环
                if not res_dict:
                    break

                self.save_res_dict_df_to_db(appender=appender, res_dict=res_dict, columns=columns, types=types)

                print(f"[Dolphindb] Obey tushare rule, wait {TUSHARE_CRAWL_STOP_INTERVEL}s ...")
                time.sleep(TUSHARE_CRAWL_STOP_INTERVEL)

        [t.join() for t in t_list]

        self.save_res_dict_df_to_db(appender=appender, res_dict=res_dict, columns=columns, types=types)

        return error_code_list

    @staticmethod
    def save_res_dict_df_to_db(
            appender: ddb.PartitionedTableAppender, res_dict: Dict[str, pd.DataFrame],
            columns: List[str], types: List[str]
    ) -> None:
        '''

        :param appender:
        :param res_dict:
        :param columns:
        :param types:
        :return: 聚合res_dict value 生成的 res_df
        '''
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
        print(f"[Dolphindb] Write len:{len(res_df)} data to db cost:{time.perf_counter() - start_time:.2f}s")

        res_dict.clear()

    def download_stock_moneyflow_hsgt_table(self):
        """
        通过python api 创建分区表, 与script不同, 这里需要先有 Dataframe 数据, 然后根据 df 去创建分区表
        :return:
        """
        from avalon.datafeed.xcsc_tushare import get_moneyflow_hsgt_df

        db_path = stock_moneyflow_hsgt_table.db_path
        table_name = stock_moneyflow_hsgt_table.name

        if self.session.existsTable(dbUrl=db_path, tableName=table_name):
            self.session.dropTable(dbPath=db_path, tableName=table_name)
            print(f"[Dolphindb] Python api append table need to recreate table")
            print(f"[Dolphindb] Drop table, db_path:{db_path} table_name: {table_name}")

        # 已经在 self.init_stock_daily_db 中通过 sql语句 创建了 分区数据库
        # 所以该处省略
        # year_range = np.array(pd.date_range(start=f'{self.start_year}', end=f"{self.end_year}", freq='YS'),
        #                       dtype='datetime64[M]')
        # self.db = self.session.database(
        #     dbName='tushare', partitionType=keys.RANGE, partitions=year_range,
        #     dbPath=db_path
        # )

        table_df = get_moneyflow_hsgt_df()
        table_df['trade_date'] = pd.to_datetime(table_df['trade_date'])
        table_df['id'] = np.arange(0, len(table_df))
        table = self.session.table(data=table_df)

        db = self.session.database(dbPath=db_path)

        if self.engine == Engine.TSDB:
            db.createPartitionedTable(
                table=table, tableName=table_name,
                partitionColumns='trade_date',
                sortColumns=['id', 'trade_date'],
                keepDuplicates='FIRST',
            )
        elif self.engine == Engine.OLAP:
            db.createPartitionedTable(
                table=table, tableName=table_name,
                partitionColumns='trade_date'
            )

        re = self.session.loadTable(tableName=table_name, dbPath=db_path)
        re.append(table)

    def download_stock_fin_table(self, start_idx: int = -1, end_idx: int = 10_0000):
        """
        通过python api 创建分区表, 与script不同, 这里需要先有 Dataframe 数据, 然后根据 df 去创建分区表
        :return:
        """
        from avalon.datafeed.xcsc_tushare.stock import get_all_stock_fin_df

        db_path = stock_fin_table.db_path
        table_name = stock_fin_table.name

        if self.session.existsTable(dbUrl=db_path, tableName=table_name):
            self.session.dropTable(dbPath=db_path, tableName=table_name)
            print(f"[Dolphindb] Python api append table need to recreate table")
            print(f"[Dolphindb] Drop table, db_path:{db_path} table_name: {table_name}")

        # 已经在 self.init_stock_daily_db 中通过 sql语句 创建了 分区数据库
        # 所以该处省略
        # year_range = np.array(pd.date_range(start=f'{self.start_year}', end=f"{self.end_year}", freq='YS'),
        #                       dtype='datetime64[M]')
        # self.db = self.session.database(
        #     dbName='tushare', partitionType=keys.RANGE, partitions=year_range,
        #     dbPath=db_path
        # )

        table_df = get_all_stock_fin_df(start_idx=start_idx, end_idx=end_idx)
        table_df['trade_date'] = pd.to_datetime(table_df['trade_date'])
        table = self.session.table(data=table_df)

        db = self.session.database(dbPath=db_path)

        if self.engine == Engine.TSDB:
            db.createPartitionedTable(
                table=table, tableName=table_name,
                partitionColumns=['jj_code', 'trade_date'],  # 注意这个分区数据库是复合分区
                sortColumns=['jj_code', 'trade_date'],
                keepDuplicates='FIRST',
            )
        elif self.engine == Engine.OLAP:
            db.createPartitionedTable(
                table=table, tableName=table_name,
                partitionColumns='trade_date'
            )

        re = self.session.loadTable(tableName=table_name, dbPath=db_path)
        re.append(table)

    def download_stock_pingji_table(
            self,
            start_date: str = "2005-01-01",
            end_date: str = datetime.datetime.now().date().strftime("%Y-%m-%d")
    ):
        """
        只能获取到 2017-01-01 的数据
        通过python api 创建分区表, 与script不同, 这里需要先有 Dataframe 数据, 然后根据 df 去创建分区表
        :param end_date:
        :param start_date:
        :return:
        """
        from avalon.datafeed.akshare.stock.pingji_spider import get_stock_analyst_rating

        db_path = stock_pingji_table.db_path
        table_name = stock_pingji_table.name

        if self.session.existsTable(dbUrl=db_path, tableName=table_name):
            self.session.dropTable(dbPath=db_path, tableName=table_name)
            print(f"[Dolphindb] Python api append table need to recreate table")
            print(f"[Dolphindb] Drop table, db_path:{db_path} table_name: {table_name}")

        # 已经在 self.init_stock_daily_db 中通过 sql语句 创建了 分区数据库
        # 所以该处省略
        # year_range = np.array(pd.date_range(start=f'{self.start_year}', end=f"{self.end_year}", freq='YS'),
        #                       dtype='datetime64[M]')
        # self.db = self.session.database(
        #     dbName='tushare', partitionType=keys.RANGE, partitions=year_range,
        #     dbPath=db_path
        # )

        table_df = get_stock_analyst_rating(start=start_date, end=end_date)
        table_df['trade_date'] = pd.to_datetime(table_df['trade_date'])
        table = self.session.table(data=table_df)

        db = self.session.database(dbPath=db_path)

        if self.engine == Engine.TSDB:
            db.createPartitionedTable(
                table=table, tableName=table_name,
                partitionColumns=['jj_code', 'trade_date'],  # 注意这个分区数据库是复合分区
                sortColumns=['jj_code', 'trade_date'],
                keepDuplicates='FIRST',
            )
        elif self.engine == Engine.OLAP:
            db.createPartitionedTable(
                table=table, tableName=table_name,
                partitionColumns='trade_date'
            )

        re = self.session.loadTable(tableName=table_name, dbPath=db_path)
        re.append(table)

    def save_dimension_table(self, dim_table: DimensionTable):
        """
        维度表, 用于存储元信息, 简短的表
        :param dim_table:
        :return:
        """

        db_path = dim_table.db_path
        table_name = dim_table.name

        if dim_table.df.empty:
            print(f"[Dolphindb] Fail to save dimension table, {table_name} to {db_path}, table.df is empty")
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
        print(f"[Dolphindb] Success save dimension table, {table_name} to {db_path}")

    def download_stock_moneyflow_daily_table(self):
        '''
        akshare 只有最近3个多月的数据
        :return:
        '''
        from avalon.datafeed.akshare.stock.moneyflow import update_stock_daily_moneyflow_res_dict_thread

        res_dict = dict()
        update_stock_daily_moneyflow_res_dict_thread(
            stock_basic_df=self.get_dimension_table_df(StockBasicTable, from_db=False),
            res_dict=res_dict
        )
        self.save_res_dict_to_db_table(partition_table=stock_moneyflow_daily_table, res_dict=res_dict)

    def download_industry_moneyflow_daily_table(self):
        '''
        akshare 只有最近3个多月的数据
        :return:
        '''
        from gulf.akshare.stock.moneyflow import update_industry_daily_moneyflow_res_dict_thread

        res_dict = dict()
        update_industry_daily_moneyflow_res_dict_thread(
            stock_basic_df=self.get_dimension_table_df(StockBasicTable, from_db=False),
            res_dict=res_dict
        )
        self.save_res_dict_to_db_table(partition_table=industry_moneyflow_daily_table, res_dict=res_dict)

    def download_index_daily_table(
            self, partition_table: PartitionTable = index_concept_daily_table,
            start_date=None, end_date=None
    ):

        df = None
        table_name = partition_table.name
        if table_name == "index_concept_daily_table":
            from gulf.akshare.stock.index_concept import get_stock_index_concept_em_daily_df
            df = get_stock_index_concept_em_daily_df(start_date=start_date, end_date=end_date)
        elif table_name == "index_industry_daily_table":
            from gulf.akshare.stock.index_industry import get_stock_index_industry_em_daily_df
            df = get_stock_index_industry_em_daily_df(start_date=start_date, end_date=end_date)

        self.save_res_dict_to_db_table(partition_table=partition_table, res_dict={table_name: df})

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
        print(f"[Dolphindb] Write len:{len(res_df)} data to db cost:{time.perf_counter() - start_time:.2f}s")

        res_dict.clear()

    def download_all(self):
        start_time = time.perf_counter()
        extra_code_list = ['688409.SH', '688132.SH', '688448.SH', '688275.SH', '688428.SH', '688387.SH', '688392.SH',
                           '688252.SH', '688459.SH', '688073.SH', '603163.SH', '688137.SH', '603057.SH', '301363.SZ',
                           '301369.SZ', '301331.SZ', '301366.SZ', '301319.SZ', '301316.SZ', '301313.SZ', '301176.SZ',
                           '301326.SZ', '301227.SZ', '301285.SZ', '001299.SZ', '001322.SZ', '001298.SZ', '001332.SZ',
                           '001269.SZ', '001255.SZ', '000508.SZ']

        ################################################
        # # Downloading stock_daily_table by iter code: 5146it [35:14,  2.43it/s]
        error_code_list = self.download_stock_daily_code_by_code(
            # start_idx=-1,  # 增加了分步写入, 不会内存爆表, 所以可以不用手动分段了
            # end_idx=10,
            # specify_code_list=extra_code_list,
            start_date=self.start_datetime.strftime("%Y%m%d"),  # 从 start_date 开始到现在的所有 code
            table=stock_daily_table
        )
        # 上面下载遇到问题的，重新下一次
        error_code_list = self.download_stock_daily_code_by_code(
            # start_idx=-1,  # 增加了分步写入, 不会内存爆表, 所以可以不用手动分段了
            # end_idx=10,
            specify_code_list=error_code_list,
            start_date=self.start_datetime.strftime("%Y%m%d"),  # 从 start_date 开始到现在的所有 code
            table=stock_daily_table
        )

        ################################
        # self.download_stock_fin_table()
        # self.download_stock_pingji_table()

        ######################################
        ## Downloading hk_hold_table by iter date: 1580it [02:43,  9.65it/s]
        # from avalon.datafeed.xcsc_tushare import get_hk_hold_df
        # self.download_daily_code_by_date(
        #     # end_idx=3,  # 补全最近3个交易日的数据
        #     tushare_func_list=[get_hk_hold_df],
        #     table=hk_hold_table
        # )

        ####################################
        # download_stock_moneyflow_hsgt_table func time_cost -> 0.80s
        # self.download_stock_moneyflow_hsgt_table()
        # download_stock_moneyflow_daily_table func time cost -> 522s
        self.download_stock_moneyflow_daily_table()

        ###################################
        # time cost: 114.37s/ 10.76s
        self.download_bond_daily_table()

        print(f"[Dolphindb] Download all time cost: {(time.perf_counter() - start_time):.2f}s")
        # print(error_code_list)

    def update_all_dimension_table(self):
        index_hushen300_daily = self.get_dimension_table_df(IndexHS300DailyTable, from_db=False)
        trade_calender = self.get_dimension_table_df(TradeCalenderTable, from_db=False)

        stock_basic_df = self.get_dimension_table_df(StockBasicTable, from_db=False)

        index_hushen300_members = self.get_dimension_table_df(IndexHS300MembersTable, from_db=False)

        bond_basic_df = self.get_dimension_table_df(BondBasicTable, from_db=False)
        bond_redeem_df = self.get_dimension_table_df(BondRedeemTable, from_db=False)
        bond_delist_df = self.get_dimension_table_df(BondDelistTable, from_db=False)
        bond_index_df = self.get_dimension_table_df(BondIndexTable, from_db=False)

    def get_tables(self, db_path: str):
        return self.session.run(
            f"""
            db = database('{db_path}');
            getTables(db);
        """
        )

    def get_all_dfs_tables(self):
        r_dict = {}
        for dfs in self.init_db_path_dict.keys():
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

    db.download_industry_moneyflow_daily_table()
    # db.download_stock_moneyflow_daily_table()
    # db.update_all_dimension_table()
    # db.download_all()

    # df = db.get_all_dfs_tables_meta()
    # print(1)

    # db.download_index_daily_table()
    # db.download_index_daily_table(partition_table=index_industry_daily_table)

    # db.session.dropDatabase(BondBasicTable.db_path)

    # db.download_stock_daily_code_by_code(
    #     # start_idx=-1,  # 增加了分步写入, 不会内存爆表, 所以可以不用手动分段了
    #     # end_idx=10,
    #     # specify_code_list=extra_code_list,
    #     start_date="20221121",  # 从 start_date 开始到现在的所有 code
    #     table=stock_daily_table
    # )
    # db.download_stock_pingji_table()

    # db.update_all_dimension_table()
    # db.download_all()

    # db.session.dropTable(db.stock_daily_code_db_path, "stock_daily_table")

    # table_name = "stock_daily_table"
    # symbol = "SHSE.600101"
    # last_day_str = "2022.09.01"
    # trade = db.session.loadTableBySQL(
    #     tableName=table_name, dbPath=db.stock_daily_code_db_path,
    #     sql=f"select top 10 * from {table_name} where jj_code='{symbol}', trade_date<={last_day_str} order by trade_date desc"
    # )
    # trade_df = db.session.run(
    #     f"""
    #     table = loadTable('{db.stock_daily_code_db_path}', '{table_name}');
    #     select top 10 trade_date, adj_close from table where jj_code='{symbol}', trade_date<={last_day_str} order by trade_date desc;
    #     """
    # ).sort_values(by='trade_date')
    # print(1)

    # df = db.all_stocks_df

    # 清空数据库数据
    # db.init_db(clear=True)
    # 清空数据表
    # db.before_create_table(db_path=db.stock_daily_code_db_path, table_name="hk_hold_table")

    # db.session.dropTable(db.stock_daily_code_db_path, "hk_hold_table")
    # db.create_hk_hold_table()

    # clear db
    # db.session.dropDatabase(dbPath=db.stock_daily_code_db_path)
    # db.session.dropTable(dbPath=db.stock_daily_code_db_path, tableName=table_name)
    # db.session.dropTable(dbPath=db.stock_daily_db_path, tableName="moneyflow_hsgt_table")

    # db.download_daily_table()
    # df1 = db.session.loadTable(tableName="daily_table", dbPath=db.stock_daily_code_db_path).toDF()

    # db.download_daily_basic_ts_table()
    # df2 = db.session.loadTable(tableName="daily_basic_ts_table", dbPath=db.stock_daily_code_db_path).toDF()

    # db.download_hk_hold_table()
    # df3 = db.session.loadTable(tableName="hk_hold_table", dbPath=db.stock_daily_code_db_path).toDF()

    # db.download_moneyflow_hsgt_table()
    # df4 = db.session.loadTable(tableName="moneyflow_hsgt_table", dbPath=db.stock_daily_db_path).toDF()

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

    # df:pd.DataFrame = r3.merge(r2, on="jj_code")
    # df['ratio_rank'] = df['ratio'].rank()  # 大的得分高
    # df['circ_mv_rank'] = df['circ_mv'].rank(ascending=False)  # 小的得分高
    # df['sync'] = df['ratio_rank'] + df['circ_mv_rank']
    # df.sort_values(by="sync", ascending=False, inplace=True)
    print(1)
