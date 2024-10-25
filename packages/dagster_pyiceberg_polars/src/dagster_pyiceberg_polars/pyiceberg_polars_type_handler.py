from typing import Optional, Sequence, Type, Union

import polars as pl
import pyarrow as pa
from dagster import InputContext
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from dagster_pyiceberg import IcebergIOManager
from dagster_pyiceberg.handler import (
    CatalogTypes,
    IcebergBaseArrowTypeHandler,
    IcebergPyArrowTypeHandler,
    _table_reader,
)
from pyiceberg import table

PolarsTypes = Union[pl.DataFrame, pl.LazyFrame]


class IcebergPolarsTypeHandler(IcebergBaseArrowTypeHandler[PolarsTypes]):
    def from_arrow(
        self, obj: table.DataScan, target_type: Type[PolarsTypes]
    ) -> PolarsTypes:
        return (
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

    def load_input(
        self, context: InputContext, table_slice: TableSlice, connection: CatalogTypes
    ) -> PolarsTypes:
        return self.from_arrow(
            _table_reader(table_slice=table_slice, catalog=connection),
            context.dagster_type.typing_type,
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
