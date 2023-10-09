from enum import Enum
import numpy as np


class DolType(Enum):
    SYMBOL = "SYMBOL"
    NANOTIMESTAMP = "NANOTIMESTAMP"
    DOUBLE = "DOUBLE"
    INT = "INT"


class Engine(Enum):
    TSDB = "TSDB"
    OLAP = "OLAP"


DolType_2_np_dtype_dict = {
    DolType.DOUBLE.value: np.float64,
    DolType.SYMBOL.value: str,
    DolType.INT.value: np.int64,
    DolType.NANOTIMESTAMP.value: 'datetime64[ns]'
}
