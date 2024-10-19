import datetime as dt

import numpy as np
import pyarrow as pa
from dagster import Definitions, asset
from pyiceberg.catalog.sql import SqlCatalog

from dagster_pyiceberg import IcebergPyarrowIOManager, IcebergSqlCatalogConfig

CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/dag/warehouse/catalog.db"
CATALOG_WAREHOUSE = "file:///home/vscode/workspace/.tmp/dag/warehouse"

catalog = SqlCatalog(
    name="test", **{"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
)

catalog.create_namespace_if_not_exists(namespace="dagster")


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
def asset_1():
    data = {
        "date": [dt.datetime(2024, 10, i + 1, 0) for i in range(20)],
        "values": np.random.normal(0, 1, 20).tolist(),
    }
    return pa.Table.from_pydict(data)


@asset
def asset_2(asset_1: pa.Table):
    return asset_1.append_column("year", pa.array([2024] * 20))


defs = Definitions(assets=[asset_1, asset_2], resources=resources)