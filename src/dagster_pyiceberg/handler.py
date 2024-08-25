import datetime as dt
from abc import abstractmethod
from typing import Generic, Iterable, Optional, Sequence, TypeVar, Union, cast

import pendulum
import pyarrow as pa
from dagster import InputContext, OutputContext
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import (
    DbTypeHandler,
    TablePartitionDimension,
    TableSlice,
)
from pyiceberg import catalog
from pyiceberg import expressions as E
from pyiceberg import partitioning, schema
from pyiceberg import table
from pyiceberg import table as iceberg_table
from pyiceberg import transforms
from pyiceberg import types as T

U = TypeVar("U")

time_partition_dt_types = (T.TimestampType, T.DateType)
partition_types = T.StringType

ArrowTypes = Union[pa.Table, pa.RecordBatchReader]


def date_diff(start: dt.datetime, end: dt.datetime) -> pendulum.Interval:
    start_ = pendulum.instance(start)
    end_ = pendulum.instance(end)
    return end_ - start_


def diff_to_transformation(
    start: dt.datetime, end: dt.datetime
) -> transforms.Transform:
    delta = date_diff(start, end)
    match delta.in_hours():
        case 1:
            return transforms.HourTransform()
        case 24:
            return transforms.DayTransform()
        case 168:
            return transforms.DayTransform()  # No week transform available
        case _:
            if delta.in_months() == 1:
                return transforms.MonthTransform()
            else:
                raise NotImplementedError(
                    f"Unsupported time window: {delta.in_words()}"
                )


class IcebergTypeHandler(DbTypeHandler[U], Generic[U]):

    @abstractmethod
    def from_arrow(self, obj: table.DataScan, target_type: type): ...

    @abstractmethod
    def to_arrow(self, obj: U) -> pa.RecordBatchReader: ...

    def handle_output(
        self,
        context: OutputContext,
        table_slice: TableSlice,
        obj: U,
        catalog: catalog.MetastoreCatalog,
    ):
        """Stores pyarrow types in Iceberg table"""
        metadata = context.definition_metadata or {}  # noqa
        data = self.to_arrow(obj)

        _table_writer(table_slice=table_slice, data=data, catalog=catalog)

    def load_input(
        self,
        context: InputContext,
        table_slice: TableSlice,
        catalog: catalog.MetastoreCatalog,
    ) -> U:
        """Loads the input as a pyarrow Table"""
        return self.from_arrow(
            _table_reader(table_slice=table_slice, catalog=catalog),
            context.dagster_type.typing_type,
        )


def _table_writer(
    table_slice: TableSlice, data: ArrowTypes, catalog: catalog.MetastoreCatalog
):
    table_path = f"{table_slice.schema}.{table_slice.table}"
    if catalog.table_exists(table_path):
        table = catalog.load_table(table_path)
    else:
        table = catalog.create_table(
            table_path,
            schema=data.schema,
        )
        # This is a bit tricky, we need to add partition columns to the table schema
        #  and these need transforms
        # We can base them on the partition dimensions, but optionally we can allow users
        #  to pass these as metadata to the asset
        if table_slice.partition_dimensions is not None:
            with table.update_spec() as update:
                for partition in table_slice.partition_dimensions:
                    if isinstance(partition.partitions, TimeWindow):
                        transform = diff_to_transformation(*partition.partitions)
                    else:
                        transform = transforms.IdentityTransform()
                    update.add_field(partition.partition_expr, transform=transform)

    if table_slice.partition_dimensions is not None:
        partition_filters = partition_dimensions_to_filters(
            partition_dimensions=table_slice.partition_dimensions,
            table_schema=table.schema(),
            table_partition_spec=table.spec(),
        )
        row_filter = (
            E.And(*partition_filters)
            if len(partition_filters) > 1
            else partition_filters[0]
        )
    else:
        row_filter = iceberg_table.ALWAYS_TRUE

    # An overwrite may produce zero or more snapshots based on the operation:

    #  DELETE: In case existing Parquet files can be dropped completely.
    #  REPLACE: In case existing Parquet files need to be rewritten.
    #  APPEND: In case new data is being inserted into the table.
    table.overwrite(
        df=data,
        overwrite_filter=row_filter,
    )


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
    return E.EqualTo(table_partition.partition_expr, table_partition.partitions[0])


def map_partition_spec_to_fields(
    partition_spec: partitioning.PartitionSpec, table_schema: schema.Schema
):
    """Maps partition spec to fields"""
    partition_spec_fields = {}
    for field in partition_spec.fields:
        field_name = [
            column.name
            for column in table_schema.fields
            if column.field_id == field.source_id
        ][0]
        partition_spec_fields[field.source_id] = field_name
    return partition_spec_fields


def partition_dimensions_to_filters(
    partition_dimensions: Iterable[TablePartitionDimension],
    table_schema: schema.Schema,
    table_partition_spec: Optional[partitioning.PartitionSpec] = None,
):
    """Converts dagster partitions to iceberg filters"""
    partition_filters = []
    if table_partition_spec is not None:  # Only None when writing new tables
        partition_spec_fields = map_partition_spec_to_fields(
            partition_spec=table_partition_spec, table_schema=table_schema
        )
    for partition_dimension in partition_dimensions:
        field = table_schema.find_field(partition_dimension.partition_expr)
        if table_partition_spec is not None:
            if field.field_id not in partition_spec_fields.keys():
                raise ValueError(
                    f"Table is not partitioned by field '{field.name}' with id '{field.field_id}'. Available partition fields: {partition_spec_fields}"
                )
        # NB: add timestamp tz type and time type
        if isinstance(field.field_type, time_partition_dt_types):
            filter_ = _time_window_partition_filter(table_partition=partition_dimension)
        elif isinstance(field.field_type, partition_types):
            filter_ = _partition_filter(table_partition=partition_dimension)
        else:
            raise ValueError(
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

    selected_fields = table_slice.columns if table_slice.columns is not None else ("*",)
    if table_slice.partition_dimensions is not None:
        partition_filters = partition_dimensions_to_filters(
            partition_dimensions=table_slice.partition_dimensions,
            table_schema=table.schema(),
            table_partition_spec=table.spec(),
        )
        row_filter = (
            E.And(*partition_filters)
            if len(partition_filters) > 1
            else partition_filters[0]
        )
    else:
        row_filter = iceberg_table.ALWAYS_TRUE

    return table.scan(row_filter=row_filter, selected_fields=selected_fields)
