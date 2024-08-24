from typing import TypeVar, Generic, Iterable, Union, Sequence, cast
from abc import abstractmethod
import datetime as dt

import daft
from pyiceberg import catalog, schema, types as T, expressions as E
from dagster._core.storage.db_io_manager import DbTypeHandler, TablePartitionDimension, TableSlice
from dagster._core.definitions.time_window_partitions import TimeWindow

U = TypeVar("U")

time_partition_dt_types = (T.TimestampType, T.DateType)
partition_types = (T.StringType)


class IcebergTypeHandler(DbTypeHandler[U], Generic[U]):
    
    @abstractmethod
    def from_arrow(self):
        ...
        
    @abstractmethod
    def to_arrow(self):
        ...
    
    def handle_output():
        """Stores pyarrow types in Iceberg table"""
        ...
        
    def load_input():
        """Loads the input as a pyarrow Table"""
        ...


def _time_window_partition_filter(
    table_partition: TablePartitionDimension
):
    partition = cast(TimeWindow, table_partition.partitions)
    start_dt, end_dt = partition
    if isinstance(start_dt, dt.datetime):
        start_dt = start_dt.replace(tzinfo=None)
        end_dt = end_dt.replace(tzinfo=None)
    return (
        (daft.col(table_partition.partition_expr) >= start_dt) &
        (daft.col(table_partition.partition_expr) < end_dt)
    )
    
    
def _partition_filter(
    table_partition: TablePartitionDimension
):
    partition = cast(Sequence[str], table_partition.partitions)
    if len(partition) > 1:
        raise NotImplementedError(f"Array partition values are not yet supported: '{str(T.StringType)}' / {partition}")
    return daft.col(table_partition.partition_expr) == table_partition.partitions


def partition_dimensions_to_filters(
    partition_dimensions: Iterable[TablePartitionDimension],
    table_schema: schema.Schema
):
    """Converts dagster partitions to iceberg filters"""
    partition_filters = []
    for partition_dimension in partition_dimensions:
        field = table_schema.find_field(partition_dimension.partition_expr)
        # NB: add timestamp tz type and time type
        if isinstance(field.field_type, time_partition_dt_types):
            filter_ = _time_window_partition_filter(
                table_partition=partition_dimension
            )
        elif isinstance(field.field_type, partition_types):
            filter_ = _partition_filter(
                table_partition=partition_dimension
            )
        else:
            raise NotImplementedError(f"Partitioning by field type '{str(field.field_type)}' not supported")
        partition_filters.append(filter_)
    return partition_filters


def _table_reader(table_slice: TableSlice, catalog: catalog.MetastoreCatalog) -> daft.DataFrame:
    """Reads a table slice from an iceberg table and slices it according to partitioning (if present)"""
    table_name = f"{table_slice.schema}.{table_slice.table}"
    table = catalog.load_table(table_name)
    table_schema = table.schema()
    table = table.to_daft()

    if table_slice.partition_dimensions is not None:
        partition_filters = partition_dimensions_to_filters(
            partition_dimensions=table_slice.partition_dimensions,
            table_schema=table_schema
        )
        for partition_filter in partition_filters:
            table = table.where(partition_filter)
    
    return table
