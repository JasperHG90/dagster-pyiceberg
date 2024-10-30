from typing import Dict, List, Optional, Sequence, Tuple, Union

import pyarrow as pa
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice
from dagster_pyiceberg._utils.partitions import (
    IcebergTableSpecUpdater,
    PartitionMapper,
    partition_dimensions_to_filters,
)
from dagster_pyiceberg._utils.schema import IcebergTableSchemaUpdater, SchemaDiffer
from pyiceberg import expressions as E
from pyiceberg import table
from pyiceberg import table as iceberg_table
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from tenacity import RetryError, Retrying, stop_after_attempt, wait_random

CatalogTypes = Union[SqlCatalog, RestCatalog]


def table_writer(
    table_slice: TableSlice,
    data: pa.Table,
    catalog: CatalogTypes,
    schema_update_mode: str,
    partition_spec_update_mode: str,
    dagster_run_id: str,
    dagster_partition_key: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
) -> None:
    """Writes data to an iceberg table

    Args:
        table_slice (TableSlice): dagster database IO manager table slice. This
            contains information about dagster partitions.
        data (pa.Table): PyArrow table
        catalog (CatalogTypes): PyIceberg catalogs supported by this library
        schema_update_mode (str): Whether to process schema updates on existing
            tables or error, value is either 'error' or 'update'

    Raises:
        ValueError: Raised when partition dimension metadata is not set on an
            asset but the user attempts to use partitition definitions.
        ValueError: Raised when schema update mode is set to 'error' and
            asset partition definitions on an existing table do not match
            the table partition spec.
    """
    table_path = f"{table_slice.schema}.{table_slice.table}"
    base_properties = {"created_by": "dagster", "dagster_run_id": dagster_run_id}
    # In practice, partition_dimensions is an empty list for unpartitioned assets and not None
    #  even though it's the default value.
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
        # Check if schema matches. If not, update
        IcebergTableSchemaUpdater(
            schema_differ=SchemaDiffer(
                current_table_schema=table.schema().as_arrow(),
                new_table_schema=data.schema,
            ),
            schema_update_mode=schema_update_mode,
        ).update_table_schema(table=table)
        # Check if partitions match. If not, update
        if partition_dimensions is not None:
            IcebergTableSpecUpdater(
                partition_mapping=PartitionMapper(
                    table_slice=table_slice,
                    iceberg_table_schema=table.schema(),
                    iceberg_partition_spec=table.spec(),
                ),
                partition_spec_update_mode=partition_spec_update_mode,
            ).update_table_spec(table=table)
    else:
        table = catalog.create_table(
            table_path,
            schema=data.schema,
            properties=(
                table_properties | base_properties
                if table_properties is not None
                else base_properties
            ),
        )
        if partition_dimensions is not None:
            IcebergTableSpecUpdater(
                partition_mapping=PartitionMapper(
                    table_slice=table_slice,
                    iceberg_table_schema=table.schema(),
                    iceberg_partition_spec=table.spec(),
                ),
                # When creating new tables with dagster partitions, we always update
                # the partition spec
                partition_spec_update_mode="update",
            ).update_table_spec(table=table)

    row_filter: E.BooleanExpression
    if partition_dimensions is not None:
        row_filter = get_row_filter(
            iceberg_table_schema=table.schema(),
            iceberg_partition_spec=table.spec(),
            dagster_partition_dimensions=partition_dimensions,
        )
    else:
        row_filter = iceberg_table.ALWAYS_TRUE

    overwrite_table_with_retries(
        table=table,
        df=data,
        overwrite_filter=row_filter,
        snapshot_properties=(
            base_properties | {"dagster_partition_key": dagster_partition_key}
            if dagster_partition_key is not None
            else base_properties
        ),
    )


def table_reader(table_slice: TableSlice, catalog: CatalogTypes) -> table.DataScan:
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
    if table_slice.partition_dimensions:
        row_filter = get_row_filter(
            iceberg_table_schema=table.schema(),
            iceberg_partition_spec=table.spec(),
            dagster_partition_dimensions=table_slice.partition_dimensions,
        )
    else:
        row_filter = iceberg_table.ALWAYS_TRUE

    return table.scan(row_filter=row_filter, selected_fields=selected_fields)


def get_row_filter(
    iceberg_table_schema: Schema,
    iceberg_partition_spec: PartitionSpec,
    dagster_partition_dimensions: Sequence[TablePartitionDimension],
) -> E.BooleanExpression:
    """Construct an iceberg row filter based on dagster partition dimensions
    that can be used to overwrite those specific rows in the iceberg table."""
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


def overwrite_table_with_retries(
    table: table.Table,
    df: pa.Table,
    overwrite_filter: Union[E.BooleanExpression, str],
    snapshot_properties: Optional[Dict[str, str]] = None,
    retries: int = 4,
):
    """Overwrites an iceberg table and retries on failure

    NB: This will be added in PyIceberg 0.8.0 or 0.9.0. This implementation is based
        on https://github.com/apache/iceberg-python/issues/269 and https://github.com/apache/iceberg-python/pull/330

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
                        # An overwrite may produce zero or more snapshots based on the operation:

                        #  DELETE: In case existing Parquet files can be dropped completely.
                        #  REPLACE: In case existing Parquet files need to be rewritten.
                        #  APPEND: In case new data is being inserted into the table.
                        tx.overwrite(
                            df=df,
                            overwrite_filter=overwrite_filter,
                            snapshot_properties=(
                                snapshot_properties
                                if snapshot_properties is not None
                                else {}
                            ),
                        )
                        tx.commit_transaction()
                except CommitFailedException:
                    # Do not refresh on the final try
                    if retry.retry_state.attempt_number < retries:
                        table.refresh()
    except RetryError as e:
        # Ignore PyRight error since it's a problem in tenacity
        raise RetryError(f"Commit failed after {retries} retries") from e  # type: ignore
