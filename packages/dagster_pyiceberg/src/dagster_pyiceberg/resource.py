from typing import Optional, Union, cast

from dagster import ConfigurableResource
from dagster_pyiceberg.config import IcebergRestCatalogConfig, IcebergSqlCatalogConfig
from dagster_pyiceberg.io_manager import (
    _IcebergMetastoreCatalogConfig,
    connect_to_catalog,
)
from pydantic import Field
from pyiceberg.table import Table

SupportedCatalogConfigs = Union[IcebergRestCatalogConfig, IcebergSqlCatalogConfig]


class PyIcebergTableResource(ConfigurableResource):
    """Resource for interacting with a PyIceberg table.

    Examples:
        .. code-block:: python

            from dagster import Definitions, asset
            from dagster_pyiceberg import PyIcebergTableResource, LocalConfig

            @asset
            def my_table(pyiceberg_table: PyIcebergTableResource):
                df = pyiceberg_table.load().to_pandas()

            defs = Definitions(
                assets=[my_table],
                resources={
                    "pyiceberg_table,
                    PyIcebergTableResource(
                        url="/path/to/table",
                        storage_options=LocalConfig()
                    )
                }
            )
    """

    name: str = Field(description="The name of the iceberg catalog.")
    config: SupportedCatalogConfigs = Field(
        discriminator="type",
        description="Additional configuration properties for the iceberg catalog.",
    )
    schema_: Optional[str] = Field(
        default=None,
        alias="schema",
        description="Name of the iceberg catalog schema to use.",
    )  # schema is a reserved word for pydantic
    snapshot_id: Optional[int] = Field(
        default=None,
        description="Snapshot ID that you would like to load. Default is latest.",
    )

    def load(self) -> Table:
        config_ = cast(_IcebergMetastoreCatalogConfig, self.config)
        _ = connect_to_catalog(name=self.name, config=config_)
