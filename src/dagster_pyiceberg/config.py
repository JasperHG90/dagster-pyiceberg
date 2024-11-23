from typing import Any, Dict

from dagster import Config


class IcebergCatalogConfig(Config):
    """Configuration for Iceberg Catalogs. See <https://py.iceberg.apache.org/configuration/#catalogs>
    for configuration options.

    You can configure the PyIceberg IO manager:

        1. Using a `.pyiceberg.yaml` configuration file.
        2. Through environment variables.
        3. Using the `IcebergCatalogConfig` configuration object.

    For more information about the first two configuration options, see
    <https://py.iceberg.apache.org/configuration/#setting-configuration-values>

    Example:

    ```python
    from dagster_pyiceberg.config import IcebergCatalogConfig
    from dagster_pyiceberg.io_manager.arrow import IcebergPyarrowIOManager

    warehouse_path = "/path/to/warehouse

    io_manager = IcebergPyarrowIOManager(
        name=catalog_name,
        config=IcebergCatalogConfig(properties={
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        }),
        namespace=namespace,
    )
    ```
    """

    properties: Dict[str, Any]
