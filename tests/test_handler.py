import datetime as dt

import pyarrow as pa
import pytest
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice
from pyiceberg import expressions as E
from pyiceberg import table as iceberg_table
from pyiceberg import transforms
from pyiceberg.catalog.sql import SqlCatalog

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


@pytest.fixture()
def category_table_partition_dimension_multiple() -> TablePartitionDimension:
    return TablePartitionDimension("category", ["A", "B"])


@pytest.fixture()
def partitioned_table_slice(
    datetime_table_partition_dimension: TablePartitionDimension,
    category_table_partition_dimension: TablePartitionDimension,
) -> TableSlice:
    return TableSlice(
        table="data_partitioned",
        schema="pytest",
        partition_dimensions=[
            datetime_table_partition_dimension,
            category_table_partition_dimension,
        ],
    )


@pytest.fixture()
def table_slice() -> TableSlice:
    return TableSlice(
        table="data",
        schema="pytest",
        partition_dimensions=None,
    )


@pytest.fixture()
def table_slice_with_selected_columns() -> TableSlice:
    return TableSlice(
        table="data",
        schema="pytest",
        partition_dimensions=None,
        columns=["value"],
    )


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


def test_partition_filter_fails_with_multiple(
    category_table_partition_dimension_multiple: TablePartitionDimension,
):
    category_table_partition_dimension.partitions = ["A", "B"]
    with pytest.raises(NotImplementedError):
        handler._partition_filter(category_table_partition_dimension)


def test_partition_dimensions_to_filters(
    datetime_table_partition_dimension: TablePartitionDimension,
    category_table_partition_dimension: TablePartitionDimension,
    table_partitioned: iceberg_table.Table,
):
    filters = handler.partition_dimensions_to_filters(
        partition_dimensions=[
            datetime_table_partition_dimension,
            category_table_partition_dimension,
        ],
        table_schema=table_partitioned.schema(),
        table_partition_spec=table_partitioned.spec(),
    )
    expected_filters = [
        E.And(
            E.GreaterThanOrEqual("timestamp", "2023-01-01T00:00:00"),
            E.LessThan("timestamp", "2023-01-01T01:00:00"),
        ),
        E.EqualTo("category", "A"),
    ]
    assert filters == expected_filters


def test_partitioned_table_reader(
    catalog: SqlCatalog, partitioned_table_slice: TableSlice
):
    table_ = handler._table_reader(partitioned_table_slice, catalog)
    df = table_.to_pandas()
    assert df["timestamp"].min() >= dt.datetime(2023, 1, 1, 0)
    assert df["timestamp"].max() < dt.datetime(2023, 1, 1, 1)
    assert df["category"].unique().tolist() == ["A"]


def test_table_reader(catalog: SqlCatalog, table_slice: TableSlice):
    table_ = handler._table_reader(table_slice, catalog)
    df = table_.to_pandas()
    assert df.shape[0] == 1440


def test_table_reader_with_selected_columns(
    catalog: SqlCatalog, table_slice_with_selected_columns: TableSlice
):
    table_ = handler._table_reader(table_slice_with_selected_columns, catalog)
    df = table_.to_pandas()
    assert df.shape[0] == 1440
    assert df.shape[1] == 1
    assert df.columns == ["value"]


def test_diff_to_transformation():
    transformation = handler.diff_to_transformation(
        start=dt.datetime(2023, 1, 1, 0, 0, 0),
        end=dt.datetime(2023, 1, 1, 1, 0, 0),
    )
    assert transformation == transforms.DayTransform()


def test_table_writer(catalog: SqlCatalog, data: pa.Table):
    handler._table_writer(
        table_slice=TableSlice(
            table="data_table_writer",
            schema="pytest",
            partition_dimensions=None,
        ),
        data=data,
        catalog=catalog,
    )
    assert catalog.table_exists("pytest.data_table_writer")


def test_table_writer_partitioned(catalog: SqlCatalog, data: pa.Table):
    handler._table_writer(
        table_slice=TableSlice(
            table="data_table_writer_partitioned",
            schema="pytest",
            partition_dimensions=[
                TablePartitionDimension(
                    "timestamp",
                    TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
                ),
            ],
        ),
        data=data,
        catalog=catalog,
    )
    table = catalog.load_table("pytest.data_table_writer_partitioned")  # noqa
