from contextlib import contextmanager  # noqa
from typing import Iterator, Sequence, TypedDict, cast  # noqa

from dagster import OutputContext
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import (
    DbClient,
    TablePartitionDimension,
    TableSlice,
)
from pyiceberg.catalog import MetastoreCatalog

from .config import IcebergRestCatalogConfig, IcebergSqlCatalogConfig  # noqa


class _IcebergTableIOManagerResourceConfig(TypedDict):

    name: str
    properties: dict


class IcebergDbClient(DbClient):

    @staticmethod
    def delete_table_slice(
        context: OutputContext, table_slice: TableSlice, connection: MetastoreCatalog
    ) -> None: ...

    @staticmethod
    def ensure_schema_exists(
        context: OutputContext, table_slice: TableSlice, connection: MetastoreCatalog
    ) -> None: ...

    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str:
        # The select statement here is just for illustrative purposes,
        # and is never actually executed. It does however logically correspond
        # the operation being executed.
        col_str = ", ".join(table_slice.columns) if table_slice.columns else "*"

        if (
            table_slice.partition_dimensions
            and len(table_slice.partition_dimensions) > 0
        ):
            query = f"SELECT {col_str} FROM {table_slice.schema}.{table_slice.table} WHERE\n"
            return query + _partition_where_clause(table_slice.partition_dimensions)
        else:
            return f"""SELECT {col_str} FROM {table_slice.schema}.{table_slice.table}"""

    # @staticmethod
    # @contextmanager
    # def connect(context, table_slice: TableSlice) -> Iterator[MetastoreCatalog]:
    #     resource_config = cast(
    #         _DeltaTableIOManagerResourceConfig, context.resource_config
    #     )
    #     root_uri = resource_config["root_uri"].rstrip("/")
    #     storage_options = resource_config["storage_options"]

    #     if "local" in storage_options:
    #         storage_options = storage_options["local"]
    #     elif "s3" in storage_options:
    #         storage_options = storage_options["s3"]
    #     elif "azure" in storage_options:
    #         storage_options = storage_options["azure"]
    #     elif "gcs" in storage_options:
    #         storage_options = storage_options["gcs"]
    #     else:
    #         storage_options = {}

    # client_options = resource_config.get("client_options")
    # client_options = client_options or {}

    # storage_options = {
    #     **{k: str(v) for k, v in storage_options.items() if v is not None},
    #     **{k: str(v) for k, v in client_options.items() if v is not None},
    # }
    # table_config = resource_config.get("table_config")
    # table_uri = f"{root_uri}/{table_slice.schema}/{table_slice.table}"

    # conn = TableConnection(
    #     table_uri=table_uri,
    #     storage_options=storage_options or {},
    #     table_config=table_config,
    # )

    # yield conn


def _partition_where_clause(
    partition_dimensions: Sequence[TablePartitionDimension],
) -> str:
    return " AND\n".join(
        (
            _time_window_where_clause(partition_dimension)
            if isinstance(partition_dimension.partitions, TimeWindow)
            else _static_where_clause(partition_dimension)
        )
        for partition_dimension in partition_dimensions
    )


def _time_window_where_clause(table_partition: TablePartitionDimension) -> str:
    partition = cast(TimeWindow, table_partition.partitions)
    start_dt, end_dt = partition
    start_dt_str = start_dt.isoformat()
    end_dt_str = end_dt.isoformat()
    return f"""{table_partition.partition_expr} >= '{start_dt_str}' AND {table_partition.partition_expr} < '{end_dt_str}'"""


def _static_where_clause(table_partition: TablePartitionDimension) -> str:
    partitions = ", ".join(f"'{partition}'" for partition in table_partition.partitions)
    return f"""{table_partition.partition_expr} in ({partitions})"""
