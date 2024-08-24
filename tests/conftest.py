import datetime as dt

import numpy as np
import pyarrow as pa
import pytest
from pyiceberg import table as iceberg_table
from pyiceberg import transforms as T
from pyiceberg.catalog.sql import SqlCatalog


@pytest.fixture(scope="session")
def warehouse_path(tmp_path_factory) -> str:
    dir = tmp_path_factory.mktemp("warehouse")
    return str(dir)


@pytest.fixture(scope="session")
def catalog(warehouse_path: str) -> SqlCatalog:
    return SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )


@pytest.fixture(scope="session")
def namespace(catalog: SqlCatalog) -> str:
    catalog.create_namespace("pytest")
    return "pytest"


@pytest.fixture(scope="session")
def data() -> pa.Table:
    np.random.seed(876)
    N = 1440
    d = {
        "timestamp": pa.array(
            [
                dt.datetime(2023, 1, 1, 0, 0, 0) + dt.timedelta(minutes=i)
                for i in range(N)
            ]
        ),
        "category": pa.array([np.random.choice(["A", "B", "C"]) for _ in range(N)]),
        "value": pa.array(np.random.normal(size=N)),
    }
    return pa.Table.from_pydict(d)


@pytest.fixture(scope="session")
def schema(data: pa.Table) -> pa.Schema:
    return data.schema


@pytest.fixture(scope="session", autouse=True)
def create_catalog_table(catalog: SqlCatalog, namespace: str, schema: pa.Schema):
    catalog.create_table(f"{namespace}.data", schema=schema)


@pytest.fixture(scope="session", autouse=True)
def create_catalog_table_partitioned(
    catalog: SqlCatalog, namespace: str, schema: pa.Schema
):
    partitioned_table = catalog.create_table(
        f"{namespace}.data_partitioned", schema=schema
    )
    with partitioned_table.update_spec() as update:
        update.add_field(source_column_name="timestamp", transform=T.HourTransform())


@pytest.fixture
def table(catalog: SqlCatalog, namespace: str) -> iceberg_table.Table:
    return catalog.load_table(f"{namespace}.data")


@pytest.fixture
def table_partitioned(catalog: SqlCatalog, namespace: str) -> iceberg_table.Table:
    return catalog.load_table(f"{namespace}.data_partitioned")
