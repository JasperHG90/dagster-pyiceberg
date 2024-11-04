import enum
from abc import abstractmethod
from contextlib import contextmanager  # noqa
from typing import (  # noqa
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Type,
    TypedDict,
    Union,
    cast,
)

from dagster import (
    InputContext,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    OutputContext,
    TimeWindowPartitionsDefinition,
)
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


class SchemaUpdateMode(enum.Enum):
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
    schema_update_mode: SchemaUpdateMode


def _connect_to_catalog(
    name: str,
    config: _IcebergMetastoreCatalogConfig,
) -> CatalogTypes:
    """Connect to the iceberg catalog."""

    if "sql" in config:
        catalog = SqlCatalog(name=name, **config["sql"]["properties"])
    elif "rest" in config:
        catalog = RestCatalog(name=name, **config["rest"]["properties"])
    else:
        raise NotImplementedError(
            f"Catalog type '{next(iter(config.keys()))}' not implemented"
        )
    return catalog


class CustomDbIOManager(DbIOManager):
    """Works exactly like the DbIOManager, but overrides the _get_table_slice method
    to allow user to pass multiple partitions to select. This happens e.g. when a user makes
    a mapping from partition A to partition B, where A is partitioned on two dimensions and
    B is partitioned on only one dimension.

        See:
            - Issue: <https://github.com/dagster-io/dagster/issues/17838>
            - Open PR: <https://github.com/dagster-io/dagster/pull/20400>
    """

    def _get_table_slice(
        self, context: Union[OutputContext, InputContext], output_context: OutputContext
    ) -> TableSlice:
        output_context_metadata = output_context.definition_metadata or {}

        schema: str
        table: str
        partition_dimensions: List[TablePartitionDimension] = []
        if context.has_asset_key:
            asset_key_path = context.asset_key.path
            table = asset_key_path[-1]
            # schema order of precedence: metadata, I/O manager 'schema' config, key_prefix
            if output_context_metadata.get("schema"):
                schema = cast(str, output_context_metadata["schema"])
            elif self._schema:
                schema = self._schema
            elif len(asset_key_path) > 1:
                schema = asset_key_path[-2]
            else:
                schema = "public"

            if context.has_asset_partitions:
                partition_expr = output_context_metadata.get("partition_expr")
                if partition_expr is None:
                    raise ValueError(
                        f"Asset '{context.asset_key}' has partitions, but no 'partition_expr'"
                        " metadata value, so we don't know what column it's partitioned on. To"
                        " specify a column, set this metadata value. E.g."
                        ' @asset(metadata={"partition_expr": "your_partition_column"}).'
                    )

                if isinstance(context.asset_partitions_def, MultiPartitionsDefinition):
                    multi_partition_key_mappings = [
                        cast(MultiPartitionKey, partition_key).keys_by_dimension
                        for partition_key in context.asset_partition_keys
                    ]
                    for part in context.asset_partitions_def.partitions_defs:
                        partitions = []
                        for multi_partition_key_mapping in multi_partition_key_mappings:
                            partition_key = multi_partition_key_mapping[part.name]
                            if isinstance(
                                part.partitions_def, TimeWindowPartitionsDefinition
                            ):
                                partitions.append(
                                    part.partitions_def.time_window_for_partition_key(
                                        partition_key
                                    )
                                )
                            else:
                                partitions.append(partition_key)

                        partition_expr_str = cast(
                            Mapping[str, str], partition_expr
                        ).get(part.name)
                        if partition_expr is None:
                            raise ValueError(
                                f"Asset '{context.asset_key}' has partition {part.name}, but the"
                                f" 'partition_expr' metadata does not contain a {part.name} entry,"
                                " so we don't know what column to filter it on. Specify which"
                                " column of the database contains data for the"
                                f" {part.name} partition."
                            )
                        partition_dimensions.append(
                            TablePartitionDimension(
                                partition_expr=cast(str, partition_expr_str),
                                partitions=partitions,
                            )
                        )
                elif isinstance(
                    context.asset_partitions_def, TimeWindowPartitionsDefinition
                ):
                    partition_dimensions.append(
                        TablePartitionDimension(
                            partition_expr=cast(str, partition_expr),
                            partitions=(
                                context.asset_partitions_time_window
                                if context.asset_partition_keys
                                else []
                            ),
                        )
                    )
                else:
                    partition_dimensions.append(
                        TablePartitionDimension(
                            partition_expr=cast(str, partition_expr),
                            partitions=context.asset_partition_keys,
                        )
                    )
        else:
            table = output_context.name
            if output_context_metadata.get("schema"):
                schema = cast(str, output_context_metadata["schema"])
            elif self._schema:
                schema = self._schema
            else:
                schema = "public"

        return TableSlice(
            table=table,
            schema=schema,
            database=self._database,
            partition_dimensions=partition_dimensions,
            columns=(context.definition_metadata or {}).get("columns"),
        )


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
        yield _connect_to_catalog(
            name=resource_config["name"], config=resource_config["config"]
        )


class IcebergIOManager(ConfigurableIOManagerFactory):
    """Base class for an IO manager definition that reads inputs from and writes outputs to Iceberg tables.

    Examples:
        .. code-block:: python

            from dagster_pyiceberg import IcebergIOManager, IcebergSqlCatalogConfig
            from dagster_pyiceberg_pandas import IcebergPandasTypeHandler
            from dagster._core.storage.db_io_manager import DbTypeHandler

            class MyIcebergLakeIOManager(IcebergIOManager):
                @staticmethod
                def type_handlers() -> Sequence[DbTypeHandler]:
                    return [IcebergPandasTypeHandler()]

            @asset(
                key_prefix=["my_schema"]  # will be used as the schema (parent folder) in the iceberg catalog
            )
            def my_table() -> pd.DataFrame:  # the name of the asset will be the table name
                ...

            defs = Definitions(
                assets=[my_table],
                resources={"io_manager": MyIcebergLakeIOManager(
                    name="my_iceberg_lake",
                    config=IcebergSqlCatalogConfig(
                        properties={"uri": <SQL-URI>, "warehouse": <WAREHOUSE-LOCATION>}
                    ),
                )},
            )

    If you do not provide a schema, Dagster will determine a schema based on the assets and ops using
    the I/O Manager. For assets, the schema will be determined from the asset key, as in the above example.
    For ops, the schema can be specified by including a "schema" entry in output metadata. If none
    of these is provided, the schema will default to "public".

    .. code-block:: python

        @op(
            out={"my_table": Out(metadata={"schema": "my_schema"})}
        )
        def make_my_table() -> pd.DataFrame:
            ...

    To only use specific columns of a table as input to a downstream op or asset, add the metadata "columns" to the
    In or AssetIn.

    .. code-block:: python

        @asset(
            ins={"my_table": AssetIn("my_table", metadata={"columns": ["a"]})}
        )
        def my_table_a(my_table: pd.DataFrame):
            # my_table will just contain the data from column "a"
            ...

    """

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
    schema_update_mode: SchemaUpdateMode = Field(
        default=SchemaUpdateMode.error,
        description="Logic to use when updating an iceberg table schema.",
    )

    @staticmethod
    @abstractmethod
    def type_handlers() -> Sequence[DbTypeHandler]: ...

    @staticmethod
    def default_load_type() -> Optional[Type]:
        return None

    def create_io_manager(self, context) -> CustomDbIOManager:
        self.config.model_dump()
        return CustomDbIOManager(
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
