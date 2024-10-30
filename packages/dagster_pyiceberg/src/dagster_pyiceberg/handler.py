from abc import abstractmethod
from typing import Generic, Sequence, Type, TypeVar, Union, cast

import pyarrow as pa
from dagster import InputContext, OutputContext
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from dagster_pyiceberg._utils import CatalogTypes, table_reader, table_writer
from pyiceberg.table import DataScan

U = TypeVar("U")

ArrowTypes = Union[pa.Table, pa.RecordBatchReader]


class IcebergBaseArrowTypeHandler(DbTypeHandler[U], Generic[U]):
    """
    Base class for PyIceberg type handlers

    To implement a new type handler (e.g. pandas), subclass this class and implement
    the `from_arrow` and `to_arrow` methods. These methods convert between the target
    type (e.g. pandas DataFrame) and pyarrow Table. You must also declare a property
    `supported_types` that lists the types that the handler supports.
    See `IcebergPyArrowTypeHandler` for an example.

    Target types are determined in the user code by type annotating the output of
    a dagster asset.
    """

    @abstractmethod
    def from_arrow(self, obj: DataScan, target_type: type) -> U: ...

    # TODO: deltalake uses record batch reader, as `write_deltalake` takes this as
    #  an input, see <https://delta-io.github.io/delta-rs/api/delta_writer/>
    #  but this is not supported by pyiceberg I think. We need to check this.
    @abstractmethod
    def to_arrow(self, obj: U) -> pa.Table: ...

    def handle_output(
        self,
        context: OutputContext,
        table_slice: TableSlice,
        obj: U,
        connection: CatalogTypes,
    ):
        """Stores pyarrow types in Iceberg table"""
        metadata = context.definition_metadata or {}  # noqa
        resource_config = context.resource_config or {}

        # NB: not checking properties here, except for protected properties
        table_properties_usr = metadata.get("table_properties", {})
        for k, v in table_properties_usr.items():
            if k in ["created_by", "run_id"]:
                raise KeyError(
                    f"Table properties cannot contain the following keys: {k}"
                )

        partition_spec_update_mode = cast(
            str, resource_config["partition_spec_update_mode"]
        )
        schema_update_mode = cast(str, resource_config["schema_update_mode"])

        data = self.to_arrow(obj)

        table_writer(
            table_slice=table_slice,
            data=data,
            catalog=connection,
            partition_spec_update_mode=partition_spec_update_mode,
            schema_update_mode=schema_update_mode,
            dagster_run_id=context.run_id,
            dagster_partition_key=(
                context.partition_key if context.has_asset_partitions else None
            ),
            table_properties=table_properties_usr,
        )

    def load_input(
        self,
        context: InputContext,
        table_slice: TableSlice,
        connection: CatalogTypes,
    ) -> U:
        """Loads the input as a pyarrow Table"""
        return self.from_arrow(
            table_reader(table_slice=table_slice, catalog=connection),
            context.dagster_type.typing_type,
        )


class IcebergPyArrowTypeHandler(IcebergBaseArrowTypeHandler[ArrowTypes]):
    """Type handler that converts data between Iceberg tables and pyarrow Tables"""

    def from_arrow(self, obj: DataScan, target_type: Type[ArrowTypes]) -> ArrowTypes:
        if target_type == pa.Table:
            return obj.to_arrow()
        else:
            return obj.to_arrow_batch_reader()

    def to_arrow(self, obj: ArrowTypes) -> pa.Table:
        return obj

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return (pa.Table, pa.RecordBatchReader)
