import pyarrow as pa
import pytest
from dagster import asset, materialize
from pyiceberg.catalog.sql import SqlCatalog

from dagster_pyiceberg import IcebergPyarrowIOManager, IcebergSqlCatalogConfig


@pytest.fixture
def io_manager(tmp_path) -> IcebergPyarrowIOManager:
    return IcebergPyarrowIOManager(
        name="test",
        config=IcebergSqlCatalogConfig(
            properties={
                "uri": f"sqlite:///{str(tmp_path)}/pyiceberg_catalog.db",
                "warehouse": f"file://{str(tmp_path)}",
            }
        ),
        schema="dagster",
    )


@pytest.fixture
def sql_catalog(io_manager):
    return SqlCatalog(
        name="test", **io_manager.config.properties  # NB: must match name in IO manager
    )


@pytest.fixture(autouse=True)
def create_schema(sql_catalog, io_manager):
    sql_catalog.create_namespace("dagster")


@asset(key_prefix=["my_schema"])
def b_df() -> pa.Table:
    return pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})


@asset(key_prefix=["my_schema"])
def b_plus_one(b_df: pa.Table) -> pa.Table:
    return b_df.set_column(0, "a", pa.array([2, 3, 4]))


def test_iceberg_io_manager_with_assets(tmp_path, sql_catalog, io_manager):
    resource_defs = {"io_manager": io_manager}

    for _ in range(2):
        res = materialize([b_df, b_plus_one], resources=resource_defs)
        assert res.success

        table = sql_catalog.load_table("dagster.b_df")
        out_df = table.scan().to_arrow()
        assert out_df["a"].to_pylist() == [1, 2, 3]

        dt = sql_catalog.load_table("dagster.b_plus_one")
        out_dt = dt.scan().to_arrow()
        assert out_dt["a"].to_pylist() == [2, 3, 4]
