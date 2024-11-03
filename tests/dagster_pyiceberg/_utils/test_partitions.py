import datetime as dt
from unittest import mock

import pytest
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice
from dagster_pyiceberg._utils import partitions
from pyiceberg import expressions as E
from pyiceberg import partitioning as iceberg_partitioning
from pyiceberg import schema as iceberg_schema
from pyiceberg import table as iceberg_table
from pyiceberg import transforms
from pyiceberg import types as T


def test_time_window_partition_filter(
    datetime_table_partition_dimension: TablePartitionDimension,
):
    expected_filter = [
        E.GreaterThanOrEqual("timestamp", "2023-01-01T00:00:00"),
        E.LessThan("timestamp", "2023-01-01T01:00:00"),
    ]
    filter_ = partitions.time_window_partition_filter(
        datetime_table_partition_dimension, T.TimestampType
    )
    assert filter_ == expected_filter


def test_partition_filter(category_table_partition_dimension: TablePartitionDimension):
    expected_filter = E.EqualTo("category", "A")
    filter_ = partitions.partition_filter(category_table_partition_dimension)
    assert filter_ == expected_filter


def test_partition_filter_fails_with_multiple(
    category_table_partition_dimension_multiple: TablePartitionDimension,
):
    with pytest.raises(NotImplementedError):
        partitions.partition_filter(category_table_partition_dimension_multiple)


def test_partition_dimensions_to_filters(
    datetime_table_partition_dimension: TablePartitionDimension,
    category_table_partition_dimension: TablePartitionDimension,
    table_partitioned: iceberg_table.Table,
):
    filters = partitions.partition_dimensions_to_filters(
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


def test_iceberg_to_dagster_partition_mapper_new_fields(
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    table_ = "handler_data_multi_partitioned_update_schema_change"
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1, 1, name="timestamp", transform=transforms.HourTransform()
        ),
    )
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
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
    new_partitions = partitions.PartitionMapper(
        iceberg_table_schema=iceberg_table_schema,
        iceberg_partition_spec=spec,
        table_slice=table_slice,
    ).new()
    assert len(new_partitions) == 1
    assert new_partitions[0].partition_expr == "category"


def test_iceberg_to_dagster_partition_mapper_changed_time_partition(
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    table_ = "handler_data_multi_partitioned_update_schema_change"
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1, 1, name="timestamp", transform=transforms.HourTransform()
        ),
    )
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[
            # Changed from hourly to daily
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
        ],
    )
    updated_partitions = partitions.PartitionMapper(
        iceberg_table_schema=iceberg_table_schema,
        iceberg_partition_spec=spec,
        table_slice=table_slice,
    ).updated()
    assert len(updated_partitions) == 1
    assert updated_partitions[0].partition_expr == "timestamp"


def test_iceberg_to_dagster_partition_mapper_deleted(
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    table_ = "handler_data_multi_partitioned_update_schema_change"
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1, 1, name="timestamp", transform=transforms.HourTransform()
        ),
        iceberg_partitioning.PartitionField(
            2, 2, name="category", transform=transforms.IdentityTransform()
        ),
    )
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[],
    )
    deleted_partitions = partitions.PartitionMapper(
        iceberg_table_schema=iceberg_table_schema,
        iceberg_partition_spec=spec,
        table_slice=table_slice,
    ).deleted()

    assert len(deleted_partitions) == 2
    assert sorted(p.name for p in deleted_partitions) == ["category", "timestamp"]


def test_iceberg_table_spec_updater_delete_field(
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    table_ = "handler_spec_updater_delete"
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1, 1, name="timestamp", transform=transforms.HourTransform()
        ),
        iceberg_partitioning.PartitionField(
            2, 2, name="category", transform=transforms.IdentityTransform()
        ),
    )
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
            ),
        ],
    )
    spec_updater = partitions.IcebergTableSpecUpdater(
        partition_mapping=partitions.PartitionMapper(
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
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    table_ = "handler_spec_updater_update"
    spec = iceberg_partitioning.PartitionSpec(
        iceberg_partitioning.PartitionField(
            1, 1, name="timestamp", transform=transforms.HourTransform()
        ),
    )
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
        ],
    )
    spec_updater = partitions.IcebergTableSpecUpdater(
        partition_mapping=partitions.PartitionMapper(
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
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    table_ = "handler_spec_updater_add"
    spec = iceberg_partitioning.PartitionSpec()
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
        ],
    )
    spec_updater = partitions.IcebergTableSpecUpdater(
        partition_mapping=partitions.PartitionMapper(
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
    namespace: str,
    iceberg_table_schema: iceberg_schema.Schema,
):
    table_ = "handler_spec_updater_fails"
    spec = iceberg_partitioning.PartitionSpec()
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
        ],
    )
    spec_updater = partitions.IcebergTableSpecUpdater(
        partition_mapping=partitions.PartitionMapper(
            iceberg_table_schema=iceberg_table_schema,
            iceberg_partition_spec=spec,
            table_slice=table_slice,
        ),
        partition_spec_update_mode="error",
    )
    mock_iceberg_table = mock.MagicMock()
    with pytest.raises(ValueError, match="Partition spec update mode is set to"):
        spec_updater.update_table_spec(table=mock_iceberg_table)


def test_iceberg_table_spec_updater_fails_with_bad_data_type(
    namespace: str,
):
    # Situation: user returns e.g. a pandas DataFrame with a timestamp column
    #  that is of type string, not datetime.
    #  User partitions on this column, so we update the partition spec. However,
    #  this is not really possible since the column is of the wrong type.
    iceberg_table_schema = iceberg_schema.Schema(
        T.NestedField(1, "timestamp", T.StringType()),
        T.NestedField(
            2,
            "category",
            T.StringType(),
        ),
    )
    table_ = "handler_spec_updater_add_wrong_column_type"
    spec = iceberg_partitioning.PartitionSpec()
    table_slice = TableSlice(
        table=table_,
        schema=namespace,
        partition_dimensions=[
            TablePartitionDimension(
                "timestamp",
                TimeWindow(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)),
            ),
        ],
    )
    spec_updater = partitions.IcebergTableSpecUpdater(
        partition_mapping=partitions.PartitionMapper(
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
