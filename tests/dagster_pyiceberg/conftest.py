import datetime as dt
import random

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog


@pytest.fixture(scope="session", autouse=True)
def warehouse_path(tmp_path_factory) -> str:
    dir_ = tmp_path_factory.mktemp("warehouse")
    return str(dir_.resolve())


@pytest.fixture(scope="session", autouse=True)
def catalog(warehouse_path: str) -> SqlCatalog:
    return SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )


@pytest.fixture(scope="session", autouse=True)
def namespace(catalog: SqlCatalog) -> str:
    catalog.create_namespace("pytest")
    return "pytest"


@pytest.fixture(scope="session")
def data() -> pa.Table:
    random.seed(876)
    N = 1440
    d = {
        "timestamp": pa.array(
            [
                dt.datetime(2023, 1, 1, 0, 0, 0) + dt.timedelta(minutes=i)
                for i in range(N)
            ]
        ),
        "category": pa.array([random.choice(["A", "B", "C"]) for _ in range(N)]),
        "value": pa.array(random.uniform(0, 1) for _ in range(N)),
    }
    return pa.Table.from_pydict(d)


@pytest.fixture(scope="session")
def schema(data: pa.Table) -> pa.Schema:
    return data.schema
