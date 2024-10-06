from abc import abstractmethod
from contextlib import contextmanager  # noqa
from typing import Dict, Iterator, Optional, Sequence, Type, TypedDict, cast  # noqa

from dagster import Field, OutputContext
from dagster._config.pythonic_config import ConfigurableIOManagerFactory
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import (
    DbClient,
    DbIOManager,
    DbTypeHandler,
    TablePartitionDimension,
    TableSlice,
)
from pyiceberg.catalog import MetastoreCatalog
from pyiceberg.catalog.sql import SqlCatalog

from .config import IcebergRestCatalogConfig, IcebergSqlCatalogConfig  # noqa


class _IcebergCatalogProperties(TypedDict, total=False):

    sql_catalog: Dict[str, str]


class _IcebergTableIOManagerResourceConfig(TypedDict):

    name: str
    properties: _IcebergCatalogProperties


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

    @staticmethod
    @contextmanager
    def connect(context, table_slice: TableSlice) -> Iterator[MetastoreCatalog]:
        resource_config = cast(
            _IcebergTableIOManagerResourceConfig, context.resource_config
        )
        properties = resource_config["properties"]

        name = resource_config["name"]

        conn = SqlCatalog(name=name, **properties)

        yield conn


class BaseIcebergIOManager(ConfigurableIOManagerFactory):

    name: str = Field(description="The name of the iceberg catalog")
    properties: IcebergSqlCatalogConfig = Field(
        description="Additional configuration properties for the iceberg catalog"
    )

    @staticmethod
    @abstractmethod
    def type_handlers() -> Sequence[DbTypeHandler]: ...

    @staticmethod
    def default_load_type() -> Optional[Type]:
        return None

    def create_io_manager(self, context) -> DbIOManager:
        self.properties.dict()
        return DbIOManager(
            db_client=IcebergDbClient(),
            database="iceberg",
            schema="dagster",
            type_handlers=self.type_handlers(),
            default_load_type=self.default_load_type(),
            io_manager_name="IcebergIOManager",
        )

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
