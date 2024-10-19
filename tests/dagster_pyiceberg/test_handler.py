import datetime as dt
import pathlib as plb

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice
from dagster_pyiceberg import handler
from pyiceberg import expressions as E
from pyiceberg import partitioning as iceberg_partitioning
from pyiceberg import schema as iceberg_schema
from pyiceberg import table as iceberg_table
from pyiceberg import transforms
from pyiceberg import types as T
from pyiceberg.catalog.sql import SqlCatalog


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
        partition_dimensions=[],
    )


@pytest.fixture()
def table_slice_with_selected_columns() -> TableSlice:
    return TableSlice(
        table="data",
        schema="pytest",
        partition_dimensions=[],
        columns=["value"],
    )


@pytest.fixture(scope="module")
def create_catalog_table_partitioned_update(
    catalog: SqlCatalog, namespace: str, schema: pa.Schema
):
    partitioned_table = catalog.create_table(
        f"{namespace}.data_partitioned_update", schema=schema
    )
    with partitioned_table.update_spec() as update:
        update.add_field(
            source_column_name="timestamp",
            transform=transforms.HourTransform(),
            partition_field_name="timestamp",
        )
        update.add_field(
            source_column_name="category",
            transform=transforms.IdentityTransform(),
            partition_field_name="category",
        )


@pytest.fixture(scope="module")
def add_data_to_table(
    catalog: SqlCatalog,
    create_catalog_table_partitioned_update,
    namespace: str,
    data: pa.Table,
):
    catalog.load_table(f"{namespace}.data_partitioned_update").append(data)


def test_time_window_partition_filter(
    datetime_table_partition_dimension: TablePartitionDimension,
):
    expected_filter = [
        E.GreaterThanOrEqual("timestamp", "2023-01-01T00:00:00"),
        E.LessThan("timestamp", "2023-01-01T01:00:00"),
    ]
    filter_ = handler._time_window_partition_filter(
        datetime_table_partition_dimension, T.TimestampType
    )
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
        E.GreaterThanOrEqual("timestamp", "2023-01-01T00:00:00"),
        E.LessThan("timestamp", "2023-01-01T01:00:00"),
        E.EqualTo("category", "A"),
    ]
    assert filters == expected_filters


def test_partitioned_table_reader(
    catalog: SqlCatalog, partitioned_table_slice: TableSlice
):
    table_ = handler._table_reader(partitioned_table_slice, catalog)
    data_ = table_.to_arrow().to_pydict()
    assert min(data_["timestamp"]) >= dt.datetime(2023, 1, 1, 0)
    assert max(data_["timestamp"]) < dt.datetime(2023, 1, 1, 1)
    assert list(set(data_["category"])) == ["A"]


def test_table_reader(catalog: SqlCatalog, table_slice: TableSlice):
    table_ = handler._table_reader(table_slice, catalog)
    data_ = table_.to_arrow().to_pydict()
    assert len(data_["timestamp"]) == 1440


def test_table_reader_with_selected_columns(
    catalog: SqlCatalog, table_slice_with_selected_columns: TableSlice
):
    table_ = handler._table_reader(table_slice_with_selected_columns, catalog)
    data_ = table_.to_arrow().to_pydict()
    assert len(data_["value"]) == 1440
    assert len(data_) == 1
    assert list(data_.keys()) == ["value"]


@pytest.mark.parametrize(
    "start, end, expected_transformation",
    [
        (
            dt.datetime(2023, 1, 1, 0, 0, 0),
            dt.datetime(2023, 1, 1, 1, 0, 0),
            transforms.HourTransform(),
        ),
        (
            dt.datetime(2023, 1, 1, 0, 0, 0),
            dt.datetime(2023, 1, 2, 0, 0, 0),
            transforms.DayTransform(),
        ),
        (
            dt.datetime(2023, 1, 1, 0, 0, 0),
            dt.datetime(2023, 1, 8, 0, 0, 0),
            transforms.DayTransform(),
        ),
        (
            dt.datetime(2023, 1, 1, 0, 0, 0),
            dt.datetime(2023, 2, 1, 0, 0, 0),
            transforms.MonthTransform(),
        ),
    ],
)
def test_diff_to_transformation(start, end, expected_transformation):
    transformation = handler.diff_to_transformation(
        start=start,
        end=end,
    )
    assert transformation == expected_transformation


def test_diff_to_transformation_fails():
    with pytest.raises(NotImplementedError):
        handler.diff_to_transformation(
            start=dt.datetime(2023, 1, 1, 0, 0, 0),
            end=dt.datetime(2023, 1, 1, 0, 0, 1),
        )


def test_table_writer(catalog: SqlCatalog, data: pa.Table):
    handler._table_writer(
        table_slice=TableSlice(
            table="data_table_writer",
            schema="pytest",
            # In assets that are not partitioned, this value is not None but an empty list.
            #  bit confusing since the type is optional and default value is None
            partition_dimensions=[],
        ),
        data=data,
        catalog=catalog,
        schema_update_mode="update",
    )
    assert catalog.table_exists("pytest.data_table_writer")


def test_table_writer_partitioned(catalog: SqlCatalog, data: pa.Table):
    # Works similar to # https://docs.dagster.io/integrations/deltalake/reference#storing-multi-partitioned-assets
    # Need to subset the data.
    data = data.filter(
        (pc.field("timestamp") >= dt.datetime(2023, 1, 1, 0))
        & (pc.field("timestamp") < dt.datetime(2023, 1, 1, 1))
    )
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
        schema_update_mode="update",
    )
    table = catalog.load_table("pytest.data_table_writer_partitioned")
    partition_field_names = [f.name for f in table.spec().fields]
    assert partition_field_names == ["timestamp_hour"]
    assert len(table.scan().to_arrow().to_pydict()["value"]) == 60


def test_table_writer_multi_partitioned(catalog: SqlCatalog, data: pa.Table):
    # Works similar to # https://docs.dagster.io/integrations/deltalake/reference#storing-multi-partitioned-assets
    # Need to subset the data.
    data = data.filter(
        (pc.field("category") == "A")
        & (pc.field("timestamp") >= dt.datetime(2023, 1, 1, 0))
        & (pc.field("timestamp") < dt.datetime(2023, 1, 1, 1))
    )
    handler._table_writer(
        table_slice=TableSlice(
            table="data_table_writer_multi_partitioned",
            schema="pytest",
            partition_dimensions=[
                TablePartitionDimension(
                    "timestamp",
                    TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
                ),
                TablePartitionDimension(
                    "category",
                    ["A"],
                ),
            ],
        ),
        data=data,
        catalog=catalog,
        schema_update_mode="update",
    )
    table = catalog.load_table("pytest.data_table_writer_multi_partitioned")
    partition_field_names = [f.name for f in table.spec().fields]
    assert partition_field_names == ["timestamp_hour", "category"]
    assert len(table.scan().to_arrow().to_pydict()["value"]) == 23


def test_table_writer_multi_partitioned_update(catalog: SqlCatalog, data: pa.Table):
    # Works similar to # https://docs.dagster.io/integrations/deltalake/reference#storing-multi-partitioned-assets
    # Need to subset the data.
    data = data.filter(
        (pc.field("category") == "A")
        & (pc.field("timestamp") >= dt.datetime(2023, 1, 1, 0))
        & (pc.field("timestamp") < dt.datetime(2023, 1, 1, 1))
    ).to_pydict()
    data["value"] = pa.array([10.0] * len(data["value"]))
    data = pa.Table.from_pydict(data)
    handler._table_writer(
        table_slice=TableSlice(
            table="data_multi_partitioned_update",
            schema="pytest",
            partition_dimensions=[
                TablePartitionDimension(
                    "timestamp",
                    TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
                ),
                TablePartitionDimension(
                    "category",
                    ["A"],
                ),
            ],
        ),
        data=data,
        catalog=catalog,
        schema_update_mode="update",
    )
    table = catalog.load_table("pytest.data_multi_partitioned_update")
    data_out = (
        table.scan(
            E.And(
                E.And(
                    E.GreaterThanOrEqual("timestamp", "2023-01-01T00:00:00"),
                    E.LessThan("timestamp", "2023-01-01T01:00:00"),
                ),
                E.EqualTo("category", "A"),
            )
        )
        .to_arrow()
        .to_pydict()
    )
    assert all([v == 10 for v in data_out["value"]])


def test_table_writer_multi_partitioned_update_schema_change(
    warehouse_path: str, catalog: SqlCatalog, data: pa.Table
):
    handler._table_writer(
        table_slice=TableSlice(
            table="data_multi_partitioned_update_schema_change",
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
        schema_update_mode="update",
    )
    data_ = data.filter(
        (pc.field("category") == "A")
        & (pc.field("timestamp") >= dt.datetime(2023, 1, 1, 0))
        & (pc.field("timestamp") < dt.datetime(2023, 1, 1, 1))
    )
    handler._table_writer(
        table_slice=TableSlice(
            table="data_multi_partitioned_update_schema_change",
            schema="pytest",
            partition_dimensions=[
                TablePartitionDimension(
                    "timestamp",
                    TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
                ),
                TablePartitionDimension(
                    "category",
                    ["A"],
                ),
            ],
        ),
        data=data_,
        catalog=catalog,
        schema_update_mode="update",
    )
    path_to_dwh = (
        plb.Path(warehouse_path)
        / "pytest.db"
        / "data_multi_partitioned_update_schema_change"
        / "data"
        / "timestamp_hour=2023-01-01-00"
    )
    categories = sorted([p.name for p in path_to_dwh.glob("*") if p.is_dir()])
    assert categories == ["category=A", "category=B", "category=C"]
    assert (
        len(
            catalog.load_table("pytest.data_multi_partitioned_update_schema_change")
            .scan()
            .to_arrow()
            .to_pydict()["value"]
        )
        == 1440
    )


def test_table_writer_multi_partitioned_update_schema_change_error(
    warehouse_path: str, catalog: SqlCatalog, data: pa.Table
):
    handler._table_writer(
        table_slice=TableSlice(
            table="data_multi_partitioned_update_schema_change_error",
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
        schema_update_mode="update",
    )
    data_ = data.filter(
        (pc.field("category") == "A")
        & (pc.field("timestamp") >= dt.datetime(2023, 1, 1, 0))
        & (pc.field("timestamp") < dt.datetime(2023, 1, 1, 1))
    )
    with pytest.raises(ValueError, match="Partition dimensions do not match"):
        handler._table_writer(
            table_slice=TableSlice(
                table="data_multi_partitioned_update_schema_change_error",
                schema="pytest",
                partition_dimensions=[
                    TablePartitionDimension(
                        "timestamp",
                        TimeWindow(
                            dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)
                        ),
                    ),
                    TablePartitionDimension(
                        "category",
                        ["A"],
                    ),
                ],
            ),
            data=data_,
            catalog=catalog,
            schema_update_mode="error",
        )


def test_partition_update_differ_no_changes():
    schema = iceberg_schema.Schema(
        T.NestedField(
            1,
            "timestamp",
            T.TimestampType(),
        ),
        T.NestedField(
            2,
            "category",
            T.StringType(),
        ),
    )
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1, 1, name="timestamp", transform=transforms.HourTransform()
        ),
    )
    table_slice = TableSlice(
        table="data_multi_partitioned_update_schema_change",
        schema="pytest",
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
            ),
            TablePartitionDimension(
                "category",
                ["A"],
            ),
        ],
    )
    new_partitions = handler.PartitionUpdateDiffer(
        iceberg_table_schema=schema,
        iceberg_partition_spec=spec,
        table_slice=table_slice,
    ).diff()
    assert len(new_partitions) == 1
    assert new_partitions[0].partition_expr == "category"
