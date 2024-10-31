from typing import Optional, Sequence, Type, Union

import polars as pl
import pyarrow as pa
from dagster._core.storage.db_io_manager import DbTypeHandler
from dagster_pyiceberg import IcebergIOManager
from dagster_pyiceberg.handler import (
    IcebergBaseArrowTypeHandler,
    IcebergPyArrowTypeHandler,
)
from pyiceberg import table

PolarsTypes = Union[pl.DataFrame, pl.LazyFrame]


class IcebergPolarsTypeHandler(IcebergBaseArrowTypeHandler[PolarsTypes]):
    def from_arrow(
        self, obj: table.DataScan, target_type: Type[PolarsTypes]
    ) -> PolarsTypes:
        return (  # type: ignore
            pl.from_arrow(obj.to_arrow())
            if target_type == pl.DataFrame
            else pl.from_arrow(obj.to_arrow_batch_reader())
        )

    def to_arrow(self, obj: PolarsTypes) -> pa.Table:
        return (
            obj.collect().to_arrow()
            if isinstance(obj, pl.LazyFrame)
            else obj.to_arrow()
        )

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return [pl.DataFrame, pl.LazyFrame]


class IcebergPolarsIOManager(IcebergIOManager):
    """An IO manager definition that reads inputs from and writes outputs to Iceberg tables using PyArrow.

    Examples:
        .. code-block:: python

            import pandas as pd
            import polars as pl
            from dagster import Definitions, asset
            from dagster_pyiceberg import IcebergSqlCatalogConfig
            from dagster_pyiceberg_polars import IcebergPolarsIOManager

            CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/select_columns/catalog.db"
            CATALOG_WAREHOUSE = (
                "file:///home/vscode/workspace/.tmp/examples/select_columns/warehouse"
            )

            resources = {
                "io_manager": IcebergPolarsIOManager(
                    name="test",
                    config=IcebergSqlCatalogConfig(
                        properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
                    ),
                    schema="dagster",
                )
            }

            @asset
            def iris_dataset() -> pl.DataFrame:
                return pl.from_pandas(
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
        return [IcebergPolarsTypeHandler(), IcebergPyArrowTypeHandler()]

    @staticmethod
    def default_load_type() -> Optional[Type]:
        return pl.DataFrame
