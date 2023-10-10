from typing import Union

from gulf.dolphindb.tables.partition.schema import Schema


class PartitionTable:
    def __init__(self, name: str, db_path: str, schema: Union[Schema, None]):
        self.name = name
        self.db_path = db_path
        self.schema = schema
