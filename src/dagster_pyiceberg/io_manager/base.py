import enum
from abc import abstractmethod
from contextlib import contextmanager  # noqa
from typing import Dict, Iterator, Optional, Sequence, Type, TypedDict, cast  # noqa

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
from pyiceberg.catalog import Catalog, load_catalog

from dagster_pyiceberg._db_io_manager import CustomDbIOManager
from dagster_pyiceberg.config import IcebergCatalogConfig  # noqa


class PartitionSpecUpdateMode(enum.Enum):
    error = "error"
    update = "update"


class SchemaUpdateMode(enum.Enum):
    error = "error"
    update = "update"


class DbIoManagerImplementation(enum.Enum):
    default = "default"
    custom = "custom"


class _IcebergCatalogProperties(TypedDict):

    properties: Dict[str, str]


class _IcebergTableIOManagerResourceConfig(TypedDict):

    name: str
    config: Optional[_IcebergCatalogProperties]
    schema_: Optional[str]
    db_io_manager: DbIoManagerImplementation
    partition_spec_update_mode: PartitionSpecUpdateMode
    schema_update_mode: SchemaUpdateMode


class IcebergDbClient(DbClient):

    @staticmethod
    def delete_table_slice(
        context: OutputContext, table_slice: TableSlice, connection: Catalog
    ) -> None: ...

    @staticmethod
    def ensure_schema_exists(
        context: OutputContext, table_slice: TableSlice, connection: Catalog
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
    def connect(context, table_slice: TableSlice) -> Iterator[Catalog]:
        resource_config = cast(
            _IcebergTableIOManagerResourceConfig, context.resource_config
        )
        # Config passed as env variables or using config file.
        #  See: https://py.iceberg.apache.org/configuration/
        if resource_config["config"] is None:
            yield load_catalog(name=resource_config["name"])
        else:
            yield load_catalog(
                name=resource_config["name"], **resource_config["config"]["properties"]
            )


class IcebergIOManager(ConfigurableIOManagerFactory):
    """Base class for an IO manager definition that reads inputs from and writes outputs to Iceberg tables.

    NB: you need to use the 'schema' input to specify the *namespace* of the pyiceberg table.

    Examples:

    ```python
    import pandas as pd
    import pyarrow as pa
    from dagster import Definitions, asset

    from dagster_pyiceberg.config import IcebergCatalogConfig
    from dagster_pyiceberg.io_manager.arrow import IcebergPyarrowIOManager

    CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/select_columns/catalog.db"
    CATALOG_WAREHOUSE = (
        "file:///home/vscode/workspace/.tmp/examples/select_columns/warehouse"
    )


    resources = {
        "io_manager": IcebergPyarrowIOManager(
            name="test",
            config=IcebergCatalogConfig(
                properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
            ),
            schema="dagster",
        )
    }


    @asset
    def iris_dataset() -> pa.Table:
        pa.Table.from_pandas(
            pd.read_csv(
                "https://docs.dagster.io/assets/iris.csv",
                names=[
                    "sepal_length_cm",
                    "sepal_width_cm",
                    "petal_length_cm",
                    "petal_width_cm",
                    "species",
                ],
            )
        )


    defs = Definitions(assets=[iris_dataset], resources=resources)
    ```

    If you do not provide a schema, Dagster will determine a schema based on the assets and ops using
    the I/O Manager. For assets, the schema will be determined from the asset key, as in the above example.
    For ops, the schema can be specified by including a "schema" entry in output metadata. If none
    of these is provided, the schema will default to "public".

    ```python
    @op(
        out={"my_table": Out(metadata={"schema": "my_schema"})}
    )
    def make_my_table() -> pd.DataFrame:
        ...
    ```

    To only use specific columns of a table as input to a downstream op or asset, add the metadata "columns" to the
    In or AssetIn.

    ```python
    @asset(
        ins={"my_table": AssetIn("my_table", metadata={"columns": ["a"]})}
    )
    def my_table_a(my_table: pd.DataFrame):
        # my_table will just contain the data from column "a"
        ...
    ```
    """

    name: str = Field(description="The name of the iceberg catalog.")
    config: Optional[IcebergCatalogConfig] = Field(
        description="Additional configuration properties for the iceberg catalog. See <https://py.iceberg.apache.org/configuration/>"
        " for passing these as environment variables or using a configuration file.",
        default=None,
    )
    schema_: Optional[str] = Field(
        default=None,
        alias="schema",
        description="Name of the iceberg catalog namespace to use.",
    )  # schema is a reserved word for pydantic
    db_io_manager: DbIoManagerImplementation = Field(
        default=DbIoManagerImplementation.default,
        description="The implementation of the DbIOManager to use. 'default' uses the dagster default 'DbIOManager'."
        " 'custom' uses the custom 'CustomDbIOManager' that allows you to use additional mappings. See <docs>.",
    )

    @staticmethod
    @abstractmethod
    def type_handlers() -> Sequence[DbTypeHandler]: ...

    @staticmethod
    def default_load_type() -> Optional[Type]:
        return None

    def create_io_manager(self, context) -> DbIOManager:
        if self.config is not None:
            self.config.model_dump()
        IoManagerImplementation = (
            DbIOManager
            if self.db_io_manager == DbIoManagerImplementation.default
            else CustomDbIOManager
        )
        return IoManagerImplementation(
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
