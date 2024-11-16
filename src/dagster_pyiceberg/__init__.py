from typing import Sequence

# from dagster._core.libraries import DagsterLibraryRegistry
from dagster._core.storage.db_io_manager import DbTypeHandler

from dagster_pyiceberg.config import (
    IcebergRestCatalogConfig as IcebergRestCatalogConfig,
)
from dagster_pyiceberg.config import IcebergSqlCatalogConfig as IcebergSqlCatalogConfig
from dagster_pyiceberg.handler import IcebergDaftTypeHandler as IcebergDaftTypeHandler
from dagster_pyiceberg.handler import (
    IcebergPolarsTypeHandler as IcebergPolarsTypeHandler,
)
from dagster_pyiceberg.handler import (
    IcebergPyArrowTypeHandler as IcebergPyArrowTypeHandler,
)
from dagster_pyiceberg.io_manager import IcebergIOManager as IcebergIOManager
from dagster_pyiceberg.resource import IcebergTableResource as IcebergTableResource
from dagster_pyiceberg.version import __version__ as __version__


class IcebergPyarrowIOManager(IcebergIOManager):
    """An IO manager definition that reads inputs from and writes outputs to Iceberg tables using PyArrow.

    Examples:
        .. code-block:: python

            import pandas as pd
            import pyarrow as pa
            from dagster import Definitions, asset
            from dagster_pyiceberg import IcebergPyarrowIOManager, IcebergSqlCatalogConfig

            CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/select_columns/catalog.db"
            CATALOG_WAREHOUSE = (
                "file:///home/vscode/workspace/.tmp/examples/select_columns/warehouse"
            )

            resources = {
                "io_manager": IcebergPyarrowIOManager(
                    name="test",
                    config=IcebergSqlCatalogConfig(
                        properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
                    ),
                    schema="dagster",
                )
            }

            @asset
            def iris_dataset() -> pd.DataFrame:
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

    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [IcebergPyArrowTypeHandler()]


class IcebergPolarsIOManager(IcebergIOManager):
    """An IO manager definition that reads inputs from and writes outputs to Iceberg tables using Polars.

    Examples:
        .. code-block:: python

            import pandas as pd
            import pyarrow as pa
            from dagster import Definitions, asset
            from dagster_pyiceberg import IcebergPyarrowIOManager, IcebergSqlCatalogConfig

            CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/select_columns/catalog.db"
            CATALOG_WAREHOUSE = (
                "file:///home/vscode/workspace/.tmp/examples/select_columns/warehouse"
            )

            resources = {
                "io_manager": IcebergPyarrowIOManager(
                    name="test",
                    config=IcebergSqlCatalogConfig(
                        properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
                    ),
                    schema="dagster",
                )
            }

            @asset
            def iris_dataset() -> pd.DataFrame:
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

    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [IcebergPolarsTypeHandler()]


class IcebergDaftIOManager(IcebergIOManager):
    """An IO manager definition that reads inputs from and writes outputs to Iceberg tables using Daft.

    Examples:
        .. code-block:: python

            import pandas as pd
            import pyarrow as pa
            from dagster import Definitions, asset
            from dagster_pyiceberg import IcebergPyarrowIOManager, IcebergSqlCatalogConfig

            CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/select_columns/catalog.db"
            CATALOG_WAREHOUSE = (
                "file:///home/vscode/workspace/.tmp/examples/select_columns/warehouse"
            )

            resources = {
                "io_manager": IcebergPyarrowIOManager(
                    name="test",
                    config=IcebergSqlCatalogConfig(
                        properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
                    ),
                    schema="dagster",
                )
            }

            @asset
            def iris_dataset() -> pd.DataFrame:
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

    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [IcebergDaftTypeHandler()]


# DagsterLibraryRegistry.register("dagster-pyiceberg", __version__)
