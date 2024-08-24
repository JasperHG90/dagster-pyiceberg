import datetime as dt

import pytest
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import TablePartitionDimension
from pyiceberg import expressions as E
from pyiceberg import table as iceberg_table

from dagster_pyiceberg import handler


@pytest.fixture()
def time_window() -> TimeWindow:
    return TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1))


@pytest.fixture()
def datetime_table_partition_dimension(
    time_window: TimeWindow,
) -> TablePartitionDimension:
    return TablePartitionDimension("timestamp", time_window)


@pytest.fixture()
def category_table_partition_dimension() -> TablePartitionDimension:
    return TablePartitionDimension("category", ["A"])


def test_time_window_partition_filter(
    datetime_table_partition_dimension: TablePartitionDimension,
):
    expected_filter = E.And(
        E.GreaterThanOrEqual("timestamp", "2023-01-01T00:00:00"),
        E.LessThan("timestamp", "2023-01-01T01:00:00"),
    )
    filter_ = handler._time_window_partition_filter(datetime_table_partition_dimension)
    assert filter_ == expected_filter


def test_partition_filter(category_table_partition_dimension: TablePartitionDimension):
    expected_filter = E.EqualTo("category", "A")
    filter_ = handler._partition_filter(category_table_partition_dimension)
    assert filter_ == expected_filter


def test_partition_dimensions_to_filters(
    datetime_table_partition_dimension: TablePartitionDimension,
    category_table_partition_dimension: TablePartitionDimension,
    table: iceberg_table.Table,
):
    filters = handler.partition_dimensions_to_filters(
        partition_dimensions=[
            datetime_table_partition_dimension,
            category_table_partition_dimension,
        ],
        table_schema=table.schema(),
    )
    expected_filters = [
        E.And(
            E.GreaterThanOrEqual("timestamp", "2023-01-01T00:00:00"),
            E.LessThan("timestamp", "2023-01-01T01:00:00"),
        ),
        E.EqualTo("category", "A"),
    ]
    assert filters == expected_filters
