from typing import Optional, Sequence, Type, Union

import polars as pl
import pyarrow as pa
from dagster._core.storage.db_io_manager import DbTypeHandler
from dagster_pyiceberg import IcebergIOManager
from dagster_pyiceberg.handler import (
    IcebergBaseArrowTypeHandler,
    IcebergPyArrowTypeHandler,
)
from pyiceberg import table

PolarsTypes = Union[pl.DataFrame, pl.LazyFrame]


class IcebergPolarsTypeHandler(IcebergBaseArrowTypeHandler[PolarsTypes]):
    def from_arrow(
        self, obj: table.DataScan, target_type: Type[PolarsTypes]
    ) -> PolarsTypes:
        return (  # type: ignore
            pl.from_arrow(obj.to_arrow())
            if target_type == pl.DataFrame
            else pl.from_arrow(obj.to_arrow_batch_reader())
        )

    def to_arrow(self, obj: PolarsTypes) -> pa.Table:
        return (
            obj.collect().to_arrow()
            if isinstance(obj, pl.LazyFrame)
            else obj.to_arrow()
        )

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return [pl.DataFrame, pl.LazyFrame]


class IcebergPolarsIOManager(IcebergIOManager):
    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [IcebergPolarsTypeHandler(), IcebergPyArrowTypeHandler()]

    @staticmethod
    def default_load_type() -> Optional[Type]:
        return pl.DataFrame
