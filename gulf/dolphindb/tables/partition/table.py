from typing import List, Union

from gulf.dolphindb.const import KeepDuplicate
from gulf.dolphindb.tables.partition.schema import Schema


class PartitionTable:
    def __init__(
            self, name: str, db_path: str, schema: Union[Schema, None],
            partition_columns: List[str], sort_columns: List[str], keep_duplicates: KeepDuplicate
    ):
        """

        :param name:
        :param db_path:
        :param schema:
        :param partition_columns:
        :param sort_columns: TSDB engine 排序要把时间写在最后
        :param keep_duplicates:
        """
        self.name = name
        self.db_path = db_path
        self.schema = schema
        self.partition_columns = partition_columns
        self.sort_columns = sort_columns
        self.keep_duplicates = keep_duplicates
