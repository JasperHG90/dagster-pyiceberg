from typing import Optional, Sequence, Type

import polars as pl
import pyarrow as pa
from dagster._core.storage.db_io_manager import DbTypeHandler
from dagster_pyiceberg.handler import (
    IcebergBaseArrowTypeHandler,
    IcebergPyArrowTypeHandler,
)
from dagster_pyiceberg.io_manager import IcebergIOManager
from pyiceberg import table


class IcebergPolarsTypeHandler(IcebergBaseArrowTypeHandler[pl.DataFrame]):
    def from_arrow(
        self, obj: table.DataScan, target_type: Type[pl.DataFrame]
    ) -> pl.DataFrame:
        return obj.to_polars()

    def to_arrow(self, obj: pl.DataFrame) -> pa.Table:
        return pa.Table.from_polars(obj)

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return [pl.DataFrame]


class IcebergPolarsIOManager(IcebergIOManager):
    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [IcebergPolarsTypeHandler(), IcebergPyArrowTypeHandler()]

    @staticmethod
    def default_load_type() -> Optional[Type]:
        return pl.DataFrame
