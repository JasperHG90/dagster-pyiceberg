from unittest import mock

import pyarrow as pa
import pytest
from dagster_pyiceberg._utils import schema


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
    schema_differ = schema.SchemaDiffer(
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
    schema_updater = schema.IcebergTableSchemaUpdater(
        schema_differ=schema.SchemaDiffer(
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
    schema_updater = schema.IcebergTableSchemaUpdater(
        schema_differ=schema.SchemaDiffer(
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
    schema_updater = schema.IcebergTableSchemaUpdater(
        schema_differ=schema.SchemaDiffer(
            current_table_schema=schema_current,
            new_table_schema=schema_new,
        ),
        schema_update_mode="error",
    )
    mock_iceberg_table = mock.MagicMock()
    with pytest.raises(ValueError):
        schema_updater.update_table_schema(table=mock_iceberg_table)
