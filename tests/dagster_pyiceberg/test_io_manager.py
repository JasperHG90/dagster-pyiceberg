import datetime as dt

import pyarrow as pa
import pytest
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    HourlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    asset,
    materialize,
)
from dagster_pyiceberg import IcebergPyarrowIOManager, IcebergSqlCatalogConfig
from pyiceberg.catalog.sql import SqlCatalog


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
        schema_update_mode="error",
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


@asset(
    key_prefix=["my_schema"],
    partitions_def=HourlyPartitionsDefinition(start_date=dt.datetime(2022, 1, 1, 0)),
    config_schema={"value": str},
    metadata={"partition_expr": "partition"},
)
def hourly_partitioned(context: AssetExecutionContext) -> pa.Table:
    partition = dt.datetime.strptime(context.partition_key, "%Y-%m-%d-%H:%M")
    value = context.op_execution_context.op_config["value"]

    return pa.Table.from_pydict({"partition": [partition], "value": [value], "b": [1]})


@asset(
    key_prefix=["my_schema"],
    partitions_def=DailyPartitionsDefinition(start_date="2022-01-01"),
    config_schema={"value": str},
    metadata={"partition_expr": "partition"},
)
def daily_partitioned(context: AssetExecutionContext) -> pa.Table:
    partition = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    value = context.op_execution_context.op_config["value"]

    return pa.Table.from_pydict({"partition": [partition], "value": [value], "b": [1]})


@asset(
    name="daily_partitioned",
    key_prefix=["my_schema"],
    partitions_def=HourlyPartitionsDefinition(start_date="2022-01-01-00:00"),
    config_schema={"value": str},
    metadata={"partition_expr": "partition"},
)
def daily_partitioned_schema_update(context: AssetExecutionContext) -> pa.Table:
    partition = dt.datetime.strptime(context.partition_key, "%Y-%m-%d-%H:%M")
    value = context.op_execution_context.op_config["value"]

    return pa.Table.from_pydict({"partition": [partition], "value": [value], "b": [1]})


@asset(
    key_prefix=["my_schema"],
    partitions_def=MultiPartitionsDefinition(
        partitions_defs={
            "date": DailyPartitionsDefinition(start_date="2022-01-01"),
            "category": StaticPartitionsDefinition(["a", "b", "c"]),
        }
    ),
    config_schema={"value": str},
    metadata={
        "partition_expr": {
            "date": "date",
            "category": "category",
        }
    },
)
def multi_partitioned(context: AssetExecutionContext) -> pa.Table:

    category, date = context.partition_key.split("|")
    date_parsed = dt.datetime.strptime(date, "%Y-%m-%d").date()
    value = context.op_execution_context.op_config["value"]

    return pa.Table.from_pydict(
        {"date": [date_parsed], "value": [value], "b": [1], "category": [category]}
    )


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


def test_iceberg_io_manager_with_daily_partitioned_assets(
    tmp_path, sql_catalog, io_manager
):
    resource_defs = {"io_manager": io_manager}

    for date in ["2022-01-01", "2022-01-02", "2022-01-03"]:
        res = materialize(
            [daily_partitioned],
            partition_key=date,
            resources=resource_defs,
            run_config={
                "ops": {"my_schema__daily_partitioned": {"config": {"value": "1"}}}
            },
        )
        assert res.success

    table = sql_catalog.load_table("dagster.daily_partitioned")
    assert len(table.spec().fields) == 1
    assert table.spec().fields[0].name == "partition_day"

    out_df = table.scan().to_arrow()
    assert out_df["partition"].to_pylist() == [
        dt.date(2022, 1, 3),
        dt.date(2022, 1, 2),
        dt.date(2022, 1, 1),
    ]


# def test_iceberg_io_manager_fails_on_schema_update(tmp_path, sql_catalog, io_manager):
#     resource_defs = {"io_manager": io_manager}

#     for date in ["2022-01-01", "2022-01-02", "2022-01-03"]:
#         res = materialize(
#             [daily_partitioned],
#             partition_key=date,
#             resources=resource_defs,
#             run_config={
#                 "ops": {"my_schema__daily_partitioned": {"config": {"value": "1"}}}
#             },
#         )
#         assert res.success

#     res = materialize(
#         [daily_partitioned_schema_update],
#         partition_key="2022-01-01-00:00",
#         resources=resource_defs,
#         run_config={
#             "ops": {"my_schema__daily_partitioned": {"config": {"value": "1"}}}
#         },
#     )


def test_iceberg_io_manager_with_hourly_partitioned_assets(
    tmp_path, sql_catalog, io_manager
):
    resource_defs = {"io_manager": io_manager}

    for date in ["2022-01-01-01:00", "2022-01-01-02:00", "2022-01-01-03:00"]:
        res = materialize(
            [hourly_partitioned],
            partition_key=date,
            resources=resource_defs,
            run_config={
                "ops": {"my_schema__hourly_partitioned": {"config": {"value": "1"}}}
            },
        )
        assert res.success

    table = sql_catalog.load_table("dagster.hourly_partitioned")
    assert len(table.spec().fields) == 1
    assert table.spec().fields[0].name == "partition_hour"

    out_df = table.scan().to_arrow()
    assert out_df["partition"].to_pylist() == [
        dt.datetime(2022, 1, 1, 3, 0),
        dt.datetime(2022, 1, 1, 2, 0),
        dt.datetime(2022, 1, 1, 1, 0),
    ]


def test_iceberg_io_manager_with_multipartitioned_assets(
    tmp_path, sql_catalog, io_manager
):
    resource_defs = {"io_manager": io_manager}

    for key in [
        "a|2022-01-01",
        "b|2022-01-01",
        "c|2022-01-01",
        "a|2022-01-02",
        "b|2022-01-02",
        "c|2022-01-02",
    ]:
        res = materialize(
            [multi_partitioned],
            partition_key=key,
            resources=resource_defs,
            run_config={
                "ops": {"my_schema__multi_partitioned": {"config": {"value": "1"}}}
            },
        )
        assert res.success

    table = sql_catalog.load_table("dagster.multi_partitioned")
    assert len(table.spec().fields) == 2
    assert [f.name for f in table.spec().fields] == ["category", "date_day"]

    out_df = table.scan().to_arrow()
    assert out_df["date"].to_pylist() == [
        dt.date(2022, 1, 2),
        dt.date(2022, 1, 2),
        dt.date(2022, 1, 2),
        dt.date(2022, 1, 1),
        dt.date(2022, 1, 1),
        dt.date(2022, 1, 1),
    ]
    assert out_df["category"].to_pylist() == ["c", "b", "a", "c", "b", "a"]
