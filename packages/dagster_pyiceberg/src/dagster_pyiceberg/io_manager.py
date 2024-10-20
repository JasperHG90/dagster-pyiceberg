import enum
from abc import abstractmethod
from contextlib import contextmanager  # noqa
from typing import (  # noqa
    Dict,
    Iterator,
    Optional,
    Sequence,
    Type,
    TypedDict,
    Union,
    cast,
)

from dagster import OutputContext
from dagster._config.pythonic_config import ConfigurableIOManagerFactory
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import (
    DbClient,
    DbIOManager,
    DbTypeHandler,
    TablePartitionDimension,
    TableSlice,
)
from pydantic import Field
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog

from .config import IcebergRestCatalogConfig, IcebergSqlCatalogConfig  # noqa
from .handler import CatalogTypes


class PartitionSpecUpdateMode(enum.Enum):
    error = "error"
    update = "update"


class _IcebergCatalogProperties(TypedDict):

    properties: Dict[str, str]


class _IcebergMetastoreCatalogConfig(TypedDict, total=False):

    sql: _IcebergCatalogProperties
    rest: _IcebergCatalogProperties


class _IcebergTableIOManagerResourceConfig(TypedDict):

    name: str
    config: _IcebergMetastoreCatalogConfig
    partition_spec_update_mode: PartitionSpecUpdateMode


class IcebergDbClient(DbClient):

    @staticmethod
    def delete_table_slice(
        context: OutputContext, table_slice: TableSlice, connection: CatalogTypes
    ) -> None: ...

    @staticmethod
    def ensure_schema_exists(
        context: OutputContext, table_slice: TableSlice, connection: CatalogTypes
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
    def connect(context, table_slice: TableSlice) -> Iterator[CatalogTypes]:
        resource_config = cast(
            _IcebergTableIOManagerResourceConfig, context.resource_config
        )
        config = resource_config["config"]
        name = resource_config["name"]

        if "sql" in config:
            catalog = SqlCatalog(name=name, **config["sql"]["properties"])
        elif "rest" in config:
            catalog = RestCatalog(name=name, **config["rest"]["properties"])
        else:
            raise NotImplementedError(
                f"Catalog type '{next(iter(config.keys()))}' not implemented"
            )

        yield catalog


class IcebergIOManager(ConfigurableIOManagerFactory):

    name: str = Field(description="The name of the iceberg catalog.")
    config: Union[IcebergSqlCatalogConfig, IcebergRestCatalogConfig] = Field(
        discriminator="type",
        description="Additional configuration properties for the iceberg catalog.",
    )
    schema_: Optional[str] = Field(
        default=None,
        alias="schema",
        description="Name of the iceberg catalog schema to use.",
    )  # schema is a reserved word for pydantic
    partition_spec_update_mode: PartitionSpecUpdateMode = Field(
        default=PartitionSpecUpdateMode.error,
        description="Logic to use when updating an iceberg table partition spec with non-matching dagster partitions.",
    )

    @staticmethod
    @abstractmethod
    def type_handlers() -> Sequence[DbTypeHandler]: ...

    @staticmethod
    def default_load_type() -> Optional[Type]:
        return None

    def create_io_manager(self, context) -> DbIOManager:
        self.config.model_dump()
        return DbIOManager(
            db_client=IcebergDbClient(),
            database="iceberg",
            schema=self.schema_,
            type_handlers=self.type_handlers(),
            default_load_type=self.default_load_type(),
            io_manager_name="IcebergIOManager",
        )


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
