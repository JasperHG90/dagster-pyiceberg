from functools import cached_property
from typing import List

import pyarrow as pa
from pyiceberg import table
from pyiceberg.table.update.schema import UpdateSchema


class SchemaDiffer:

    def __init__(self, current_table_schema: pa.Schema, new_table_schema: pa.Schema):
        self.current_table_schema = current_table_schema
        self.new_table_schema = new_table_schema

    @property
    def has_changes(self) -> bool:
        return not sorted(self.current_table_schema.names) == sorted(
            self.new_table_schema.names
        )

    @cached_property
    def deleted_columns(self) -> List[str]:
        return list(
            set(self.current_table_schema.names) - set(self.new_table_schema.names)
        )

    @cached_property
    def new_columns(self) -> List[str]:
        return list(
            set(self.new_table_schema.names) - set(self.current_table_schema.names)
        )


class IcebergTableSchemaUpdater:

    def __init__(
        self,
        schema_differ: SchemaDiffer,
        schema_update_mode: str,
    ):
        self.schema_update_mode = schema_update_mode
        self.schema_differ = schema_differ

    @staticmethod
    def _delete_column(update: UpdateSchema, column: str):
        try:
            update.delete_column(column)
        except ValueError:
            # Already deleted by another operation
            pass

    @staticmethod
    def _merge_schemas(update: UpdateSchema, new_table_schema: pa.Schema):
        try:
            update.union_by_name(new_table_schema)
        except ValueError:
            # Already merged by another operation
            pass

    def update_table_schema(self, table: table.Table):
        if self.schema_update_mode == "error" and self.schema_differ.has_changes:
            raise ValueError(
                "Schema spec update mode is set to 'error' but there are schema changes to the Iceberg table"
            )
        elif not self.schema_differ.has_changes:
            return
        else:
            with table.update_schema() as update:
                for column in self.schema_differ.deleted_columns:
                    self._delete_column(update, column)
                if self.schema_differ.new_columns:
                    self._merge_schemas(update, self.schema_differ.new_table_schema)
