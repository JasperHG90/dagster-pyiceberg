import datetime as dt
import pathlib as plb
from unittest import mock

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


@pytest.fixture(scope="function")
def iceberg_table_schema() -> iceberg_schema.Schema:
    return iceberg_schema.Schema(
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
        partition_spec_update_mode="update",
        dagster_run_id="hfkghdgsh467374828",
    )
    assert catalog.table_exists("pytest.data_table_writer")
    table = catalog.load_table("pytest.data_table_writer")
    assert table.properties["dagster_run_id"] == "hfkghdgsh467374828"
    assert table.properties["created_by"] == "dagster"
    assert (
        table.current_snapshot().summary.additional_properties["dagster_run_id"]
        == "hfkghdgsh467374828"
    )
    assert (
        table.current_snapshot().summary.additional_properties["created_by"]
        == "dagster"
    )


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
        partition_spec_update_mode="update",
        dagster_run_id="hfkghdgsh467374828",
    )
    table = catalog.load_table("pytest.data_table_writer_partitioned")
    partition_field_names = [f.name for f in table.spec().fields]
    assert partition_field_names == ["timestamp"]
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
        partition_spec_update_mode="update",
        dagster_run_id="hfkghdgsh467374828",
    )
    table = catalog.load_table("pytest.data_table_writer_multi_partitioned")
    partition_field_names = [f.name for f in table.spec().fields]
    assert partition_field_names == ["timestamp", "category"]
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
        partition_spec_update_mode="update",
        dagster_run_id="hfkghdgsh467374828",
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


def test_table_writer_multi_partitioned_update_partition_spec_change(
    warehouse_path: str, catalog: SqlCatalog, data: pa.Table
):
    handler._table_writer(
        table_slice=TableSlice(
            table="data_multi_partitioned_update_partition_spec_change",
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
        partition_spec_update_mode="update",
        dagster_run_id="hfkghdgsh467374828",
    )
    data_ = data.filter(
        (pc.field("category") == "A")
        & (pc.field("timestamp") >= dt.datetime(2023, 1, 1, 0))
        & (pc.field("timestamp") < dt.datetime(2023, 1, 1, 1))
    )
    handler._table_writer(
        table_slice=TableSlice(
            table="data_multi_partitioned_update_partition_spec_change",
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
        partition_spec_update_mode="update",
        dagster_run_id="hfkghdgsh467374828",
    )
    path_to_dwh = (
        plb.Path(warehouse_path)
        / "pytest.db"
        / "data_multi_partitioned_update_partition_spec_change"
        / "data"
        / "timestamp=2023-01-01-00"
    )
    categories = sorted([p.name for p in path_to_dwh.glob("*") if p.is_dir()])
    assert categories == ["category=A", "category=B", "category=C"]
    assert (
        len(
            catalog.load_table(
                "pytest.data_multi_partitioned_update_partition_spec_change"
            )
            .scan()
            .to_arrow()
            .to_pydict()["value"]
        )
        == 1440
    )


def test_table_writer_multi_partitioned_update_partition_spec_error(
    warehouse_path: str, catalog: SqlCatalog, data: pa.Table
):
    handler._table_writer(
        table_slice=TableSlice(
            table="data_multi_partitioned_update_partition_spec_error",
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
        partition_spec_update_mode="update",
        dagster_run_id="hfkghdgsh467374828",
    )
    data_ = data.filter(
        (pc.field("category") == "A")
        & (pc.field("timestamp") >= dt.datetime(2023, 1, 1, 0))
        & (pc.field("timestamp") < dt.datetime(2023, 1, 1, 1))
    )
    with pytest.raises(
        ValueError, match="Partition spec update mode is set to 'error' but there"
    ):
        handler._table_writer(
            table_slice=TableSlice(
                table="data_multi_partitioned_update_partition_spec_error",
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
            schema_update_mode="update",
            partition_spec_update_mode="error",
            dagster_run_id="hfkghdgsh467374828",
        )


def test_iceberg_table_writer_with_table_properties(
    catalog: SqlCatalog, data: pa.Table
):
    handler._table_writer(
        table_slice=TableSlice(
            table="data_iceberg_table_writer_with_table_properties",
            schema="pytest",
            partition_dimensions=[],
        ),
        data=data,
        catalog=catalog,
        schema_update_mode="update",
        partition_spec_update_mode="update",
        table_properties={
            "write.parquet.page-size-bytes": "2048",  # 2MB
            "write.parquet.page-row-limit": "10000",
        },
        dagster_run_id="hfkghdgsh467374828",
    )
    table = catalog.load_table("pytest.data_iceberg_table_writer_with_table_properties")
    assert table.properties["write.parquet.page-size-bytes"] == "2048"
    assert table.properties["write.parquet.page-row-limit"] == "10000"


def test_iceberg_to_dagster_partition_mapper_new_fields(
    iceberg_table_schema: iceberg_schema.Schema,
):
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
    new_partitions = handler.PartitionMapper(
        iceberg_table_schema=iceberg_table_schema,
        iceberg_partition_spec=spec,
        table_slice=table_slice,
    ).new()
    assert len(new_partitions) == 1
    assert new_partitions[0].partition_expr == "category"


def test_iceberg_to_dagster_partition_mapper_changed_time_partition(
    iceberg_table_schema: iceberg_schema.Schema,
):
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1, 1, name="timestamp", transform=transforms.HourTransform()
        ),
    )
    table_slice = TableSlice(
        table="data_multi_partitioned_update_schema_change",
        schema="pytest",
        partition_dimensions=[
            # Changed from hourly to daily
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
        ],
    )
    updated_partitions = handler.PartitionMapper(
        iceberg_table_schema=iceberg_table_schema,
        iceberg_partition_spec=spec,
        table_slice=table_slice,
    ).updated()
    assert len(updated_partitions) == 1
    assert updated_partitions[0].partition_expr == "timestamp"


def test_iceberg_to_dagster_partition_mapper_deleted(
    iceberg_table_schema: iceberg_schema.Schema,
):
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1, 1, name="timestamp", transform=transforms.HourTransform()
        ),
        iceberg_partitioning.PartitionField(
            2, 2, name="category", transform=transforms.IdentityTransform()
        ),
    )
    table_slice = TableSlice(
        table="data_multi_partitioned_update_schema_change",
        schema="pytest",
        partition_dimensions=[],
    )
    deleted_partitions = handler.PartitionMapper(
        iceberg_table_schema=iceberg_table_schema,
        iceberg_partition_spec=spec,
        table_slice=table_slice,
    ).deleted()

    assert len(deleted_partitions) == 2
    assert sorted(p.name for p in deleted_partitions) == ["category", "timestamp"]


def test_iceberg_table_spec_updater_delete_field(
    iceberg_table_schema: iceberg_schema.Schema,
):
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1, 1, name="timestamp", transform=transforms.HourTransform()
        ),
        iceberg_partitioning.PartitionField(
            2, 2, name="category", transform=transforms.IdentityTransform()
        ),
    )
    table_slice = TableSlice(
        table="spec_updater_delete",
        schema="pytest",
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
            ),
        ],
    )
    spec_updater = handler.IcebergTableSpecUpdater(
        partition_mapping=handler.PartitionMapper(
            iceberg_table_schema=iceberg_table_schema,
            iceberg_partition_spec=spec,
            table_slice=table_slice,
        ),
        partition_spec_update_mode="update",
    )
    mock_iceberg_table = mock.MagicMock()
    spec_updater.update_table_spec(table=mock_iceberg_table)
    mock_iceberg_table.update_spec.assert_called_once()
    mock_iceberg_table.update_spec.return_value.__enter__.return_value.remove_field.assert_called_once_with(
        name="category"
    )


def test_iceberg_table_spec_updater_update_field(
    iceberg_table_schema: iceberg_schema.Schema,
):
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1, 1, name="timestamp", transform=transforms.HourTransform()
        ),
    )
    table_slice = TableSlice(
        table="spec_updater_update",
        schema="pytest",
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
        ],
    )
    spec_updater = handler.IcebergTableSpecUpdater(
        partition_mapping=handler.PartitionMapper(
            iceberg_table_schema=iceberg_table_schema,
            iceberg_partition_spec=spec,
            table_slice=table_slice,
        ),
        partition_spec_update_mode="update",
    )
    mock_iceberg_table = mock.MagicMock()
    spec_updater.update_table_spec(table=mock_iceberg_table)
    mock_iceberg_table.update_spec.assert_called_once()
    mock_iceberg_table.update_spec.return_value.__enter__.return_value.remove_field.assert_called_once_with(
        name="timestamp"
    )
    mock_iceberg_table.update_spec.return_value.__enter__.return_value.add_field.assert_called_once_with(
        source_column_name="timestamp",
        transform=transforms.DayTransform(),
        partition_field_name="timestamp",
    )


def test_iceberg_table_spec_updater_add_field(
    iceberg_table_schema: iceberg_schema.Schema,
):
    spec = iceberg_partitioning.PartitionSpec()
    table_slice = TableSlice(
        table="spec_updater_add",
        schema="pytest",
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
        ],
    )
    spec_updater = handler.IcebergTableSpecUpdater(
        partition_mapping=handler.PartitionMapper(
            iceberg_table_schema=iceberg_table_schema,
            iceberg_partition_spec=spec,
            table_slice=table_slice,
        ),
        partition_spec_update_mode="update",
    )
    mock_iceberg_table = mock.MagicMock()
    spec_updater.update_table_spec(table=mock_iceberg_table)
    mock_iceberg_table.update_spec.assert_called_once()
    mock_iceberg_table.update_spec.return_value.__enter__.return_value.add_field.assert_called_once_with(
        source_column_name="timestamp",
        transform=transforms.DayTransform(),
        partition_field_name="timestamp",
    )


def test_iceberg_table_spec_updater_fails_with_error_update_mode(
    iceberg_table_schema: iceberg_schema.Schema,
):
    spec = iceberg_partitioning.PartitionSpec()
    table_slice = TableSlice(
        table="spec_updater_fails",
        schema="pytest",
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
        ],
    )
    spec_updater = handler.IcebergTableSpecUpdater(
        partition_mapping=handler.PartitionMapper(
            iceberg_table_schema=iceberg_table_schema,
            iceberg_partition_spec=spec,
            table_slice=table_slice,
        ),
        partition_spec_update_mode="error",
    )
    mock_iceberg_table = mock.MagicMock()
    with pytest.raises(ValueError, match="Partition spec update mode is set to"):
        spec_updater.update_table_spec(table=mock_iceberg_table)


def test_schema_differ_removed_fields():
    schema_current = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("ns")),
            pa.field("category", pa.string()),
        ]
    )
    schema_new = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("ns")),
        ]
    )
    schema_differ = handler.SchemaDiffer(
        current_table_schema=schema_current,
        new_table_schema=schema_new,
    )
    assert schema_differ.has_changes
    assert schema_differ.deleted_columns == ["category"]


def test_iceberg_schema_updater_add_column():
    schema_current = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("ns")),
            pa.field("category", pa.string()),
        ]
    )
    schema_new = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("ns")),
            pa.field("category", pa.string()),
            pa.field("value", pa.float64()),
        ]
    )
    schema_updater = handler.IcebergTableSchemaUpdater(
        schema_differ=handler.SchemaDiffer(
            current_table_schema=schema_current,
            new_table_schema=schema_new,
        ),
        schema_update_mode="update",
    )
    mock_iceberg_table = mock.MagicMock()
    schema_updater.update_table_schema(table=mock_iceberg_table)
    mock_iceberg_table.update_schema.assert_called_once()
    mock_iceberg_table.update_schema.return_value.__enter__.return_value.union_by_name.assert_called_once_with(
        schema_new
    )


def test_iceberg_schema_updater_delete_column():
    schema_current = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("ns")),
            pa.field("category", pa.string()),
            pa.field("value", pa.float64()),
        ]
    )
    schema_new = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("ns")),
            pa.field("category", pa.string()),
        ]
    )
    schema_updater = handler.IcebergTableSchemaUpdater(
        schema_differ=handler.SchemaDiffer(
            current_table_schema=schema_current,
            new_table_schema=schema_new,
        ),
        schema_update_mode="update",
    )
    mock_iceberg_table = mock.MagicMock()
    schema_updater.update_table_schema(table=mock_iceberg_table)
    mock_iceberg_table.update_schema.assert_called_once()
    mock_iceberg_table.update_schema.return_value.__enter__.return_value.delete_column.assert_called_once_with(
        "value"
    )


def test_iceberg_schema_updater_fails_with_error_update_mode():
    schema_current = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("ns")),
        ]
    )
    schema_new = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("ns")),
            pa.field("category", pa.string()),
        ]
    )
    schema_updater = handler.IcebergTableSchemaUpdater(
        schema_differ=handler.SchemaDiffer(
            current_table_schema=schema_current,
            new_table_schema=schema_new,
        ),
        schema_update_mode="error",
    )
    mock_iceberg_table = mock.MagicMock()
    with pytest.raises(ValueError):
        schema_updater.update_table_schema(table=mock_iceberg_table)
