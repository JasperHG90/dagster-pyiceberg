from typing import Optional, Sequence, Type

import pandas as pd
import pyarrow as pa
from dagster._core.storage.db_io_manager import DbTypeHandler
from dagster_pyiceberg.handler import (
    IcebergBaseArrowTypeHandler,
    IcebergPyArrowTypeHandler,
)
from dagster_pyiceberg.io_manager import IcebergIOManager
from pyiceberg import table


class IcebergPandasTypeHandler(IcebergBaseArrowTypeHandler[pd.DataFrame]):
    def from_arrow(
        self, obj: table.DataScan, target_type: Type[pd.DataFrame]
    ) -> pd.DataFrame:
        return obj.to_pandas()

    def to_arrow(self, obj: pd.DataFrame) -> pa.RecordBatchReader:
        return pa.Table.from_pandas(obj).to_reader()

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return [pd.DataFrame]


class IcebergPandasIOManager(IcebergIOManager):
    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [IcebergPandasTypeHandler(), IcebergPyArrowTypeHandler()]

    @staticmethod
    def default_load_type() -> Optional[Type]:
        return pd.DataFrame
