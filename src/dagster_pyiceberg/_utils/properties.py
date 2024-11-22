import logging
from functools import cached_property
from typing import List

from pyiceberg import table
from pyiceberg.exceptions import CommitFailedException

from dagster_pyiceberg._utils.retries import PyIcebergOperationWithRetry


def update_table_properties(
    table: table.Table, current_table_properties: dict, new_table_properties: dict
):
    PyIcebergPropertiesUpdaterWithRetry(table=table).execute(
        retries=3,
        exception_types=CommitFailedException,
        current_table_properties=current_table_properties,
        new_table_properties=new_table_properties,
    )


class PyIcebergPropertiesUpdaterWithRetry(PyIcebergOperationWithRetry):

    def operation(self, current_table_properties: dict, new_table_properties: dict):
        IcebergTablePropertiesUpdater(
            table_properties_differ=TablePropertiesDiffer(
                current_table_properties=current_table_properties,
                new_table_properties=new_table_properties,
            ),
        ).update_table_properties(self.table, table_properties=new_table_properties)


class TablePropertiesDiffer:

    def __init__(self, current_table_properties: dict, new_table_properties: dict):
        self.current_table_properties = current_table_properties
        self.new_table_properties = new_table_properties

    @property
    def has_changes(self) -> bool:
        return (
            not (
                len(self.updated_properties)
                + len(self.deleted_properties)
                + len(self.new_properties)
            )
            == 0
        )

    @cached_property
    def updated_properties(self) -> List[str]:
        updated = []
        for k in self.new_table_properties.keys():
            if (
                k in self.current_table_properties
                and self.current_table_properties[k] != self.new_table_properties[k]
            ):
                updated.append(k)
        return updated

    @cached_property
    def deleted_properties(self) -> List[str]:
        return list(
            set(self.current_table_properties.keys())
            - set(self.new_table_properties.keys())
        )

    @cached_property
    def new_properties(self) -> List[str]:
        return list(
            set(self.new_table_properties.keys())
            - set(self.current_table_properties.keys())
        )


class IcebergTablePropertiesUpdater:

    def __init__(
        self,
        table_properties_differ: TablePropertiesDiffer,
    ):
        self.table_properties_differ = table_properties_differ
        self.logger = logging.getLogger(
            "dagster_pyiceberg._utils.schema.IcebergTablePropertiesUpdater"
        )

    @property
    def deleted_properties(self):
        return self.table_properties_differ.deleted_properties

    def update_table_properties(self, table: table.Table, table_properties: dict):
        if not self.table_properties_differ.has_changes:
            return
        else:
            self.logger.debug("Updating table properties")
            with table.transaction() as tx:
                self.logger.debug(
                    f"Deleting table properties '{self.deleted_properties}'"
                )
                tx.remove_properties(*self.deleted_properties)
                self.logger.debug(
                    f"Updating table properties if applicable using '{table_properties}'"
                )
                tx.set_properties(table_properties)
