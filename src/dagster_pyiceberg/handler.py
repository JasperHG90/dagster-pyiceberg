import datetime as dt
from abc import abstractmethod
from typing import Generic, Iterable, Sequence, TypeVar, cast

from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import (
    DbTypeHandler,
    TablePartitionDimension,
    TableSlice,
)
from pyiceberg import catalog
from pyiceberg import expressions as E
from pyiceberg import schema, table
from pyiceberg import types as T

U = TypeVar("U")

time_partition_dt_types = (T.TimestampType, T.DateType)
partition_types = T.StringType


class IcebergTypeHandler(DbTypeHandler[U], Generic[U]):

    @abstractmethod
    def from_arrow(self): ...

    @abstractmethod
    def to_arrow(self): ...

    def handle_output(self):
        """Stores pyarrow types in Iceberg table"""
        ...

    def load_input(self):
        """Loads the input as a pyarrow Table"""
        ...


def _time_window_partition_filter(table_partition: TablePartitionDimension):
    partition = cast(TimeWindow, table_partition.partitions)
    start_dt, end_dt = partition
    if isinstance(start_dt, dt.datetime):
        start_dt = start_dt.replace(tzinfo=None)
        end_dt = end_dt.replace(tzinfo=None)
    return E.And(
        E.GreaterThanOrEqual(table_partition.partition_expr, start_dt.isoformat()),
        E.LessThan(table_partition.partition_expr, end_dt.isoformat()),
    )


def _partition_filter(table_partition: TablePartitionDimension):
    partition = cast(Sequence[str], table_partition.partitions)
    if len(partition) > 1:
        raise NotImplementedError(
            f"Array partition values are not yet supported: '{str(T.StringType)}' / {partition}"
        )
    return E.EqualTo(table_partition.partition_expr, table_partition.partitions)


def partition_dimensions_to_filters(
    partition_dimensions: Iterable[TablePartitionDimension], table_schema: schema.Schema
):
    """Converts dagster partitions to iceberg filters"""
    partition_filters = []
    for partition_dimension in partition_dimensions:
        field = table_schema.find_field(partition_dimension.partition_expr)
        # NB: add timestamp tz type and time type
        if isinstance(field.field_type, time_partition_dt_types):
            filter_ = _time_window_partition_filter(table_partition=partition_dimension)
        elif isinstance(field.field_type, partition_types):
            filter_ = _partition_filter(table_partition=partition_dimension)
        else:
            raise NotImplementedError(
                f"Partitioning by field type '{str(field.field_type)}' not supported"
            )
        partition_filters.append(filter_)
    return partition_filters


def _table_reader(
    table_slice: TableSlice, catalog: catalog.MetastoreCatalog
) -> table.DataScan:
    """Reads a table slice from an iceberg table and slices it according to partitioning (if present)"""
    table_name = f"{table_slice.schema}.{table_slice.table}"
    table = catalog.load_table(table_name)

    if table_slice.partition_dimensions is not None:
        partition_filters = partition_dimensions_to_filters(
            partition_dimensions=table_slice.partition_dimensions,
            table_schema=table.schema(),
        )
        table = table.scan(E.And(*partition_filters))

    return table
