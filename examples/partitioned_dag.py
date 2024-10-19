import datetime as dt
import time

import numpy as np
import pyarrow as pa
from dagster import DailyPartitionsDefinition, Definitions, Jitter, RetryPolicy, asset
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


partition = DailyPartitionsDefinition(
    start_date=dt.datetime(2024, 10, 1, 0, tzinfo=dt.timezone.utc),
    end_date=dt.datetime(2024, 10, 30, 0, tzinfo=dt.timezone.utc),
)


@asset(
    partitions_def=partition,
    metadata={"partition_expr": "date"},
    # Concurrent writes to commit log with raise CommitFailedException
    #  See: https://github.com/apache/iceberg-python/issues/1084
    # Workaround: Retry the commit operation with some jitter
    retry_policy=RetryPolicy(max_retries=3, delay=1, jitter=Jitter.PLUS_MINUS),
)
def asset_1():
    data = {
        "date": [dt.datetime(2024, 10, i + 1, 0) for i in range(20)],
        "values": np.random.normal(0, 1, 20).tolist(),
    }
    time.sleep(30)
    return pa.Table.from_pydict(data)


@asset
def asset_2(asset_1: pa.Table):
    return asset_1.append_column("year", pa.array([2024] * 20))


defs = Definitions(assets=[asset_1, asset_2], resources=resources)
