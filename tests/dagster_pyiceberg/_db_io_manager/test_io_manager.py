"""
We only support very specific cases of partition mappings
"""

import datetime as dt
from typing import Dict

import pyarrow as pa
import pytest
from dagster import (
    AssetExecutionContext,
    AssetIn,
    DailyPartitionsDefinition,
    DimensionPartitionMapping,
    MultiPartitionMapping,
    MultiPartitionsDefinition,
    MultiToSingleDimensionPartitionMapping,
    SpecificPartitionsPartitionMapping,
    StaticPartitionMapping,
    StaticPartitionsDefinition,
    asset,
    materialize,
)
from dagster_pyiceberg import IcebergPyarrowIOManager, IcebergSqlCatalogConfig
from pyiceberg.catalog.sql import SqlCatalog


@pytest.fixture
def io_manager(
    catalog_name: str, namespace: str, catalog_config_properties: Dict[str, str]
) -> IcebergPyarrowIOManager:
    return IcebergPyarrowIOManager(
        name=catalog_name,
        config=IcebergSqlCatalogConfig(properties=catalog_config_properties),
        schema=namespace,
        partition_spec_update_mode="error",
        db_io_manager="custom",
    )


daily_partitions_def = DailyPartitionsDefinition(
    start_date="2022-01-01", end_date="2022-01-10"
)

letter_partitions_def = StaticPartitionsDefinition(["a", "b", "c"])

color_partitions_def = StaticPartitionsDefinition(["red", "blue", "yellow"])

multi_partition_with_letter = MultiPartitionsDefinition(
    partitions_defs={
        "date": daily_partitions_def,
        "letter": letter_partitions_def,
    }
)

multi_partition_with_color = MultiPartitionsDefinition(
    partitions_defs={
        "date": daily_partitions_def,
        "color": color_partitions_def,
    }
)


# Base case: we have multiple partitions
@asset(
    key_prefix=["my_schema"],
    partitions_def=multi_partition_with_color,
    metadata={
        "partition_expr": {
            "date": "date_column",
            "color": "color_column",
        }
    },
)
def multi_partitioned_asset_1(context: AssetExecutionContext) -> pa.Table:

    color, date = context.partition_key.split("|")
    date_parsed = dt.datetime.strptime(date, "%Y-%m-%d").date()

    return pa.Table.from_pydict(
        {
            "date_column": [date_parsed],
            "value": [1],
            "b": [1],
            "color_column": [color],
        }
    )


# Multi-to-multi asset is supported
@asset(
    key_prefix=["my_schema"],
    partitions_def=multi_partition_with_color,
    metadata={
        "partition_expr": {
            "date": "date_column",
            "color": "color_column",
        }
    },
)
def multi_partitioned_asset_2(multi_partitioned_asset_1: pa.Table) -> pa.Table:
    return multi_partitioned_asset_1


# Multi-to-single asset is supported through MultiToSingleDimensionPartitionMapping
@asset(
    key_prefix=["my_schema"],
    partitions_def=daily_partitions_def,
    ins={
        "multi_partitioned_asset": AssetIn(
            ["my_schema", "multi_partitioned_asset_1"],
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date"
            ),
        )
    },
    metadata={
        "partition_expr": "date_column",
    },
)
def single_partitioned_asset_date(multi_partitioned_asset: pa.Table) -> pa.Table:
    return multi_partitioned_asset


@asset(
    key_prefix=["my_schema"],
    partitions_def=color_partitions_def,
    ins={
        "multi_partitioned_asset": AssetIn(
            ["my_schema", "multi_partitioned_asset_1"],
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="color"
            ),
        )
    },
    metadata={
        "partition_expr": "color_column",
    },
)
def single_partitioned_asset_color(multi_partitioned_asset: pa.Table) -> pa.Table:
    return multi_partitioned_asset


@asset(
    partitions_def=multi_partition_with_letter,
    key_prefix=["my_schema"],
    metadata={"partition_expr": {"time": "time", "letter": "letter"}},
    ins={
        "multi_partitioned_asset": AssetIn(
            ["my_schema", "multi_partitioned_asset_1"],
            partition_mapping=MultiPartitionMapping(
                {
                    "color": DimensionPartitionMapping(
                        dimension_name="letter",
                        partition_mapping=StaticPartitionMapping(
                            {"blue": "a", "red": "b", "yellow": "c"}
                        ),
                    ),
                    "date": DimensionPartitionMapping(
                        dimension_name="date",
                        partition_mapping=SpecificPartitionsPartitionMapping(
                            ["2022-01-01", "2024-01-01"]
                        ),
                    ),
                }
            ),
        )
    },
)
def mapped_multi_partition(
    context: AssetExecutionContext, multi_partitioned_asset: pa.Table
) -> pa.Table:
    letter, _ = context.partition_key.split("|")

    table_ = multi_partitioned_asset.append_column("letter", pa.array([letter]))
    return table_


def test_multi_partitioned_to_multi_partitioned_asset(
    catalog: SqlCatalog,
    io_manager: IcebergPyarrowIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for partition_key in ["red|2022-01-01", "red|2022-01-02", "red|2022-01-03"]:
        res = materialize(
            [multi_partitioned_asset_1, multi_partitioned_asset_2],
            partition_key=partition_key,
            resources=resource_defs,
        )
        assert res.success


def test_multi_partitioned_to_single_partitioned_asset_colors(
    catalog: SqlCatalog,
    io_manager: IcebergPyarrowIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for partition_key in ["red|2022-01-01", "blue|2022-01-01", "yellow|2022-01-01"]:
        res = materialize(
            [multi_partitioned_asset_1],
            partition_key=partition_key,
            resources=resource_defs,
        )
        assert res.success
    res = materialize(
        [multi_partitioned_asset_1, single_partitioned_asset_date],
        partition_key="2022-01-01",
        resources=resource_defs,
        selection=[single_partitioned_asset_date],
    )
    assert res.success


def test_multi_partitioned_to_single_partitioned_asset_dates(
    catalog: SqlCatalog,
    io_manager: IcebergPyarrowIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for partition_key in ["red|2022-01-01", "red|2022-01-02", "red|2022-01-03"]:
        res = materialize(
            [multi_partitioned_asset_1],
            partition_key=partition_key,
            resources=resource_defs,
        )
        assert res.success
    res = materialize(
        [multi_partitioned_asset_1, single_partitioned_asset_color],
        partition_key="red",
        resources=resource_defs,
        selection=[single_partitioned_asset_color],
    )
    assert res.success


# Add: experimental warning to custom db io manager
# Add: test with multiple partition selections not consecutive
def test_multi_partitioned_to_multi_partitioned_with_different_dimensions(
    catalog: SqlCatalog,
    io_manager: IcebergPyarrowIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for partition_key in ["red|2022-01-01", "blue|2022-01-01", "yellow|2022-01-01"]:
        res = materialize(
            [multi_partitioned_asset_1],
            partition_key=partition_key,
            resources=resource_defs,
        )
        assert res.success
    with pytest.raises(ValueError, match="Dates are not consecutive."):
        res = materialize(
            [multi_partitioned_asset_1, mapped_multi_partition],
            partition_key="2022-01-01|a",
            resources=resource_defs,
            selection=[mapped_multi_partition],
        )
