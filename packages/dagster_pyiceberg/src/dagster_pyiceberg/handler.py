import datetime as dt
from abc import abstractmethod
from typing import (
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import pendulum
import pyarrow as pa
from dagster import InputContext, OutputContext
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import (
    DbTypeHandler,
    TablePartitionDimension,
    TableSlice,
)
from pyiceberg import expressions as E
from pyiceberg import partitioning, schema
from pyiceberg import table
from pyiceberg import table as iceberg_table
from pyiceberg import transforms
from pyiceberg import types as T
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from tenacity import RetryError, Retrying, stop_after_attempt, wait_random

U = TypeVar("U")

time_partition_dt_types = (T.TimestampType, T.DateType)
partition_types = T.StringType

ArrowTypes = Union[pa.Table, pa.RecordBatchReader]
CatalogTypes = Union[SqlCatalog, RestCatalog]


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


class IcebergBaseArrowTypeHandler(DbTypeHandler[U], Generic[U]):

    @abstractmethod
    def from_arrow(self, obj: table.DataScan, target_type: type) -> U: ...

    # TODO: deltalake uses record batch reader, as `write_deltalake` takes this as
    #  an input, see <https://delta-io.github.io/delta-rs/api/delta_writer/>
    #  but this is not supported by pyiceberg I think. We need to check this.
    @abstractmethod
    def to_arrow(self, obj: U) -> pa.Table: ...

    def handle_output(
        self,
        context: OutputContext,
        table_slice: TableSlice,
        obj: U,
        connection: CatalogTypes,
    ):
        """Stores pyarrow types in Iceberg table"""
        metadata = context.definition_metadata or {}  # noqa
        resource_config = context.resource_config or {}  # noqa

        schema_update_mode = str(resource_config["schema_update_mode"])

        data = self.to_arrow(obj)

        _table_writer(
            table_slice=table_slice,
            data=data,
            catalog=connection,
            schema_update_mode=schema_update_mode,
        )

    def load_input(
        self,
        context: InputContext,
        table_slice: TableSlice,
        connection: CatalogTypes,
    ) -> U:
        """Loads the input as a pyarrow Table"""
        return self.from_arrow(
            _table_reader(table_slice=table_slice, catalog=connection),
            context.dagster_type.typing_type,
        )


class IcebergPyArrowTypeHandler(IcebergBaseArrowTypeHandler[ArrowTypes]):
    def from_arrow(
        self, obj: table.DataScan, target_type: Type[ArrowTypes]
    ) -> ArrowTypes:
        if target_type == pa.Table:
            return obj.to_arrow()
        else:
            return obj.to_arrow_batch_reader()

    def to_arrow(self, obj: ArrowTypes) -> pa.Table:
        return obj

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return (pa.Table, pa.RecordBatchReader)


class IcebergToDagsterPartitionMapper:

    def __init__(
        self,
        iceberg_table_schema: Schema,
        iceberg_partition_spec: PartitionSpec,
        table_slice: TableSlice,
    ):
        self.iceberg_table_schema = iceberg_table_schema
        self.iceberg_partition_spec = iceberg_partition_spec
        self.table_slice = table_slice

    def get_table_slice_partition_dimensions(
        self, allow_empty_dagster_partitions: bool = False
    ) -> Sequence[TablePartitionDimension]:
        # In practice, partition_dimensions is an empty list and not None
        partition_dimensions: Sequence[TablePartitionDimension] | None = None
        if not (
            self.table_slice.partition_dimensions is None
            or len(self.table_slice.partition_dimensions) == 0
        ):
            partition_dimensions = self.table_slice.partition_dimensions
        if partition_dimensions is None and not allow_empty_dagster_partitions:
            raise ValueError(
                "Partition dimensions are not set. Please set the 'partition_dimensions' field in the TableSlice."
            )
        return partition_dimensions if partition_dimensions is not None else []

    def get_iceberg_partition_field_by_name(
        self, name: str
    ) -> Optional[partitioning.PartitionField]:
        """Retrieve an iceberg partition field by its partition field spec name"""
        partition_field: partitioning.PartitionField | None = None
        for field in self.iceberg_partition_spec.fields:
            if field.name == name:
                partition_field = field
                break
        return partition_field

    def get_dagster_partition_dimension_names(
        self, allow_empty_dagster_partitions: bool = False
    ) -> List[str]:
        return [
            p.partition_expr
            for p in self.get_table_slice_partition_dimensions(
                allow_empty_dagster_partitions=allow_empty_dagster_partitions
            )
        ]

    @property
    def iceberg_table_partition_field_names(self) -> Dict[int, str]:
        return map_partition_spec_to_fields(
            partition_spec=self.iceberg_partition_spec,
            table_schema=self.iceberg_table_schema,
        )

    @property
    def new_partition_field_names(self) -> Set[str]:
        return set(self.get_dagster_partition_dimension_names()) - set(
            self.iceberg_table_partition_field_names.values()
        )

    @property
    def deleted_partition_field_names(self) -> Set[str]:
        return set(self.iceberg_table_partition_field_names.values()) - set(
            self.get_dagster_partition_dimension_names(
                allow_empty_dagster_partitions=True
            )
        )

    @property
    def dagster_time_partitions(self) -> List[TablePartitionDimension]:
        time_partitions = [
            p
            for p in self.get_table_slice_partition_dimensions()
            if isinstance(p.partitions, TimeWindow)
        ]
        if len(time_partitions) > 1:
            raise ValueError(
                f"Multiple time partitions found: {time_partitions}. Only one time partition is allowed."
            )
        return time_partitions

    @property
    def updated_time_partition_field(self) -> str | None:
        # The assumption is that even a multi-partitioned table will have only one time partition
        time_partition = next(iter(self.dagster_time_partitions))
        updated_field_name: str | None = None
        if time_partition is not None:
            time_partition_transformation = diff_to_transformation(
                time_partition.partitions.start, time_partition.partitions.end
            )
            current_time_partition_field = self.get_iceberg_partition_field_by_name(
                time_partition.partition_expr
            )
            if current_time_partition_field is None:
                raise ValueError(
                    f"Could not find partition field '{time_partition.partition_expr}' in the iceberg partition spec"
                )
            if time_partition_transformation != current_time_partition_field.transform:
                updated_field_name = time_partition.partition_expr
        return updated_field_name

    def diff(self) -> List[TablePartitionDimension]:
        return [
            p
            for p in self.get_table_slice_partition_dimensions()
            if p.partition_expr in self.new_partition_field_names
        ]

    def updated(self) -> List[TablePartitionDimension]:
        return [
            p
            for p in self.get_table_slice_partition_dimensions()
            if p.partition_expr == self.updated_time_partition_field
        ]

    def deleted(self) -> List[partitioning.PartitionField]:
        return [
            p
            for p in self.iceberg_partition_spec.fields
            if p.name in self.deleted_partition_field_names
        ]


def _update_table_spec(
    table: table.Table, partition_dimensions: Sequence[TablePartitionDimension]
) -> None:
    if len(partition_dimensions) == 0:
        return
    with table.update_spec() as update:
        for partition in partition_dimensions:
            if isinstance(partition.partitions, TimeWindow):
                transform = diff_to_transformation(*partition.partitions)
            else:
                transform = transforms.IdentityTransform()
            update.add_field(
                source_column_name=partition.partition_expr,
                transform=transform,
                # Name the partition field spec the same as the column name
                partition_field_name=partition.partition_expr,
            )


def _get_row_filter(
    iceberg_table_schema: Schema,
    iceberg_partition_spec: PartitionSpec,
    dagster_partition_dimensions: Sequence[TablePartitionDimension],
) -> E.BooleanExpression:
    partition_filters = partition_dimensions_to_filters(
        partition_dimensions=dagster_partition_dimensions,
        table_schema=iceberg_table_schema,
        table_partition_spec=iceberg_partition_spec,
    )
    return (
        E.And(*partition_filters)
        if len(partition_filters) > 1
        else partition_filters[0]
    )


def _table_writer(
    table_slice: TableSlice,
    data: ArrowTypes,
    catalog: CatalogTypes,
    schema_update_mode: str,
) -> None:
    table_path = f"{table_slice.schema}.{table_slice.table}"
    # In practice, partition_dimensions is an empty list for unpartitioned assets and not None
    #  even though it's the default value. To pass the static type checker, we need to ensure
    #  that table_slice.partition_dimensions is not None or an empty list.
    partition_exprs: List[str] | None = None
    partition_dimensions: Sequence[TablePartitionDimension] | None = None
    if (
        table_slice.partition_dimensions is not None
        and len(table_slice.partition_dimensions) != 0
    ):
        partition_exprs = [p.partition_expr for p in table_slice.partition_dimensions]
        if any(p is None for p in partition_exprs):
            raise ValueError(
                f"Could not map partition to partition expr, got '{partition_exprs}'."
                "Did you name your partitions correctly and provided the correct"
                "'partition_expr' in the asset metadata?"
            )
        partition_dimensions = table_slice.partition_dimensions
    if catalog.table_exists(table_path):
        table = catalog.load_table(table_path)
        # Check if partitions match. If not, update
        #  But this should be a configuration option per table
        if partition_dimensions is not None:
            new_partition_dimensions = IcebergToDagsterPartitionMapper(
                table_slice=table_slice,
                iceberg_table_schema=table.schema(),
                iceberg_partition_spec=table.spec(),
            ).diff()
            if schema_update_mode == "error" and len(new_partition_dimensions) > 0:
                raise ValueError(
                    f"Partition dimensions do not match. New partitions: {new_partition_dimensions}"
                )
            _update_table_spec(
                table=table,
                partition_dimensions=new_partition_dimensions,  # May be empty
            )
    else:
        table = catalog.create_table(
            table_path,
            schema=data.schema,
        )
        # This is a bit tricky, we need to add partition columns to the table schema
        #  and these need transforms
        # We can base them on the partition dimensions, but optionally we can allow users
        #  to pass these as metadata to the asset
        # TODO: add updates and deletes
        if partition_dimensions is not None:
            _update_table_spec(table=table, partition_dimensions=partition_dimensions)

    row_filter: E.BooleanExpression
    if partition_dimensions is not None:
        row_filter = _get_row_filter(
            iceberg_table_schema=table.schema(),
            iceberg_partition_spec=table.spec(),
            dagster_partition_dimensions=partition_dimensions,
        )
    else:
        row_filter = iceberg_table.ALWAYS_TRUE

    # An overwrite may produce zero or more snapshots based on the operation:

    #  DELETE: In case existing Parquet files can be dropped completely.
    #  REPLACE: In case existing Parquet files need to be rewritten.
    #  APPEND: In case new data is being inserted into the table.

    # TODO: use some sort of retry mechanism here
    #  See: https://github.com/apache/iceberg-python/pull/330
    #  See: https://github.com/apache/iceberg-python/issues/269
    _overwrite_table_with_retries(
        table=table,
        df=data,
        overwrite_filter=row_filter,
    )


def _overwrite_table_with_retries(
    table: table.Table,
    df: pa.Table,
    overwrite_filter: Union[E.BooleanExpression, str],
    retries: int = 4,
):
    """Overwrites an iceberg table and retries on failure

    Args:
        table (table.Table): Iceberg table
        df (pa.Table): Data to write to the table
        overwrite_filter (Union[E.BooleanExpression, str]): Filter to apply to the overwrite operation
        retries (int, optional): Max number of retries. Defaults to 4.

    Raises:
        RetryError: Raised when the commit fails after the maximum number of retries
    """
    try:
        for retry in Retrying(
            stop=stop_after_attempt(retries), reraise=True, wait=wait_random(0.1, 0.99)
        ):
            with retry:
                try:
                    with table.transaction() as tx:
                        tx.overwrite(df=df, overwrite_filter=overwrite_filter)
                        tx.commit_transaction()
                except CommitFailedException:
                    # Do not refresh on the final try
                    if retry.retry_state.attempt_number < retries:
                        table.refresh()
    except RetryError as e:
        raise RetryError(f"Commit failed after {retries} retries") from e


def _time_window_partition_filter(
    table_partition: TablePartitionDimension,
    iceberg_partition_spec_field_type: Union[
        T.DateType, T.TimestampType, T.TimeType, T.TimestamptzType
    ],
) -> List[E.BooleanExpression]:
    """Create an iceberg filter for a dagster time window partition

    Args:
        table_partition (TablePartitionDimension): Dagster time window partition
        iceberg_partition_spec_field_type (Union[T.DateType, T.TimestampType, T.TimeType, T.TimestamptzType]): Iceberg field type
         required to correctly format the partition values

    Returns:
        List[E.BooleanExpression]: List of iceberg filters with start and end dates
    """
    partition = cast(TimeWindow, table_partition.partitions)
    start_dt, end_dt = partition
    if isinstance(start_dt, dt.datetime):
        start_dt = start_dt.replace(tzinfo=None)
        end_dt = end_dt.replace(tzinfo=None)
    if isinstance(iceberg_partition_spec_field_type, T.DateType):
        # Internally, PyIceberg uses dt.date.fromisoformat to parse dates.
        #  Dagster will pass dt.datetime objects in time window partitions.
        #  but dt.date.fromisoformat cannot parse dt.datetime.isoformat strings
        start_dt = start_dt.date()
        end_dt = end_dt.date()
    return [
        E.GreaterThanOrEqual(table_partition.partition_expr, start_dt.isoformat()),
        E.LessThan(table_partition.partition_expr, end_dt.isoformat()),
    ]


def _partition_filter(table_partition: TablePartitionDimension) -> E.BooleanExpression:
    partition = cast(Sequence[str], table_partition.partitions)
    if len(partition) > 1:
        raise NotImplementedError(
            f"Array partition values are not yet supported: '{str(T.StringType)}' / {partition}"
        )
    return E.EqualTo(table_partition.partition_expr, table_partition.partitions[0])  # type: ignore


def map_partition_spec_to_fields(
    partition_spec: partitioning.PartitionSpec, table_schema: schema.Schema
):
    """Maps partition spec to fields"""
    partition_spec_fields = {}
    for field in partition_spec.fields:
        field_name = next(
            iter(
                [
                    column.name
                    for column in table_schema.fields
                    if column.field_id == field.source_id
                ]
            )
        )
        partition_spec_fields[field.source_id] = field_name
    return partition_spec_fields


def partition_dimensions_to_filters(
    partition_dimensions: Iterable[TablePartitionDimension],
    table_schema: schema.Schema,
    table_partition_spec: Optional[partitioning.PartitionSpec] = None,
) -> List[E.BooleanExpression]:
    """Converts dagster partitions to iceberg filters"""
    partition_filters = []
    partition_spec_fields: Optional[Dict[int, str]] = None
    if table_partition_spec is not None:  # Only None when writing new tables
        partition_spec_fields = map_partition_spec_to_fields(
            partition_spec=table_partition_spec, table_schema=table_schema
        )
    for partition_dimension in partition_dimensions:
        field = table_schema.find_field(partition_dimension.partition_expr)
        if partition_spec_fields is not None:
            if field.field_id not in partition_spec_fields.keys():
                raise ValueError(
                    f"Table is not partitioned by field '{field.name}' with id '{field.field_id}'. Available partition fields: {partition_spec_fields}"
                )
        # NB: add timestamp tz type and time type
        filter_: Union[E.BooleanExpression, List[E.BooleanExpression]]
        if isinstance(field.field_type, time_partition_dt_types):
            filter_ = _time_window_partition_filter(
                table_partition=partition_dimension,
                iceberg_partition_spec_field_type=field.field_type,
            )
        elif isinstance(field.field_type, partition_types):
            filter_ = _partition_filter(table_partition=partition_dimension)
        else:
            raise ValueError(
                f"Partitioning by field type '{str(field.field_type)}' not supported"
            )
        (
            partition_filters.append(filter_)
            if isinstance(filter_, E.BooleanExpression)
            else partition_filters.extend(filter_)
        )
    return partition_filters


def _table_reader(table_slice: TableSlice, catalog: CatalogTypes) -> table.DataScan:
    """Reads a table slice from an iceberg table and slices it according to partitioning (if present)"""
    if table_slice.partition_dimensions is None:
        raise ValueError(
            "Partition dimensions are not set. Please set the 'partition_dimensions' field in the TableSlice."
        )
    table_name = f"{table_slice.schema}.{table_slice.table}"
    table = catalog.load_table(table_name)
    selected_fields: Tuple[str, ...] = (
        tuple(table_slice.columns) if table_slice.columns is not None else ("*",)
    )
    row_filter: E.BooleanExpression
    # In practice, this is an empty list, not None. This screws up the type checker.
    if len(table_slice.partition_dimensions) != 0:
        row_filter = _get_row_filter(
            iceberg_table_schema=table.schema(),
            iceberg_partition_spec=table.spec(),
            dagster_partition_dimensions=table_slice.partition_dimensions,
        )
    else:
        row_filter = iceberg_table.ALWAYS_TRUE

    return table.scan(row_filter=row_filter, selected_fields=selected_fields)
