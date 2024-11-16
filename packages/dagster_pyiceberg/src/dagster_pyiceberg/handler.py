from abc import abstractmethod
from typing import Generic, Sequence, Tuple, Type, TypeVar, Union, cast

import daft as da
import polars as pl
import pyarrow as pa
from dagster import InputContext, MetadataValue, OutputContext, TableColumn, TableSchema
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from dagster_pyiceberg._utils import (
    CatalogTypes,
    DagsterPartitionToDaftSqlPredicateMapper,
    DagsterPartitionToPolarsSqlPredicateMapper,
    DagsterPartitionToPyIcebergExpressionMapper,
    table_writer,
)
from pyiceberg import expressions as E
from pyiceberg import table as ibt
from pyiceberg.table.snapshots import Snapshot

U = TypeVar("U")

ArrowTypes = Union[pa.Table, pa.RecordBatchReader]
PolarsTypes = Union[pl.LazyFrame, pl.DataFrame]


class IcebergBaseTypeHandler(DbTypeHandler[U], Generic[U]):

    @abstractmethod
    def to_data_frame(
        self, table: ibt.Table, table_slice: TableSlice, target_type: type
    ) -> U:
        pass

    @abstractmethod
    def to_arrow(self, obj: U) -> pa.Table:
        pass

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
        for k, _ in table_properties_usr.items():
            if k in [
                "created-by",
                "run-id",
                "dagster-pyiceberg-version",
                "pyiceberg-version",
            ]:
                raise KeyError(
                    f"Table properties cannot contain the following keys: {k}"
                )

        partition_spec_update_mode = cast(
            str, resource_config["partition_spec_update_mode"]
        )
        schema_update_mode = cast(str, resource_config["schema_update_mode"])

        table_writer(
            table_slice=table_slice,
            data=self.to_arrow(obj),
            catalog=connection,
            partition_spec_update_mode=partition_spec_update_mode,
            schema_update_mode=schema_update_mode,
            dagster_run_id=context.run_id,
            dagster_partition_key=(
                context.partition_key if context.has_asset_partitions else None
            ),
            table_properties=table_properties_usr,
        )

        table_ = connection.load_table(f"{table_slice.schema}.{table_slice.table}")

        current_snapshot = cast(Snapshot, table_.current_snapshot())

        context.add_output_metadata(
            {
                "table_columns": MetadataValue.table_schema(
                    TableSchema(
                        columns=[
                            TableColumn(name=f["name"], type=str(f["type"]))
                            for f in table_.schema().model_dump()["fields"]
                        ]
                    )
                ),
                **current_snapshot.model_dump(),
            }
        )

    def load_input(
        self,
        context: InputContext,
        table_slice: TableSlice,
        connection: CatalogTypes,
    ) -> U:
        """Loads the input using a dataframe implmentation"""
        return self.to_data_frame(
            table=connection.load_table(f"{table_slice.schema}.{table_slice.table}"),
            table_slice=table_slice,
            target_type=context.dagster_type.typing_type,
        )


class IcebergPyArrowTypeHandler(IcebergBaseTypeHandler[ArrowTypes]):
    """Type handler that converts data between Iceberg tables and pyarrow Tables"""

    def to_data_frame(
        self, table: ibt.Table, table_slice: TableSlice, target_type: Type[ArrowTypes]
    ) -> ArrowTypes:
        selected_fields: Tuple[str, ...] = (
            tuple(table_slice.columns) if table_slice.columns is not None else ("*",)
        )
        row_filter: E.BooleanExpression
        if table_slice.partition_dimensions:
            expressions = DagsterPartitionToPyIcebergExpressionMapper(
                partition_dimensions=table_slice.partition_dimensions,
                table_schema=table.schema(),
                table_partition_spec=table.spec(),
            ).partition_dimensions_to_filters()
            row_filter = E.And(*expressions) if len(expressions) > 1 else expressions[0]
        else:
            row_filter = ibt.ALWAYS_TRUE

        table_scan = table.scan(row_filter=row_filter, selected_fields=selected_fields)

        return (
            table_scan.to_arrow()
            if target_type == pa.Table
            else table_scan.to_arrow_batch_reader()
        )

    def to_arrow(self, obj: ArrowTypes) -> pa.Table:
        return obj

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return (pa.Table, pa.RecordBatchReader)


class IcebergPolarsTypeHandler(IcebergBaseTypeHandler[PolarsTypes]):
    """Type handler that converts data between Iceberg tables and polars DataFrames"""

    def to_data_frame(
        self, table: ibt.Table, table_slice: TableSlice, target_type: Type[PolarsTypes]
    ) -> PolarsTypes:
        selected_fields: str = (
            ",".join(table_slice.columns) if table_slice.columns is not None else "*"
        )
        row_filter: str | None = None
        if table_slice.partition_dimensions:
            expressions = DagsterPartitionToPolarsSqlPredicateMapper(
                partition_dimensions=table_slice.partition_dimensions,
                table_schema=table.schema(),
                table_partition_spec=table.spec(),
            ).partition_dimensions_to_filters()
            row_filter = " AND ".join(expressions)

        pdf = pl.scan_iceberg(source=table)

        stmt = f"SELECT {selected_fields} FROM self"
        if row_filter is not None:
            stmt += f"\nWHERE {row_filter}"
        return pdf.sql(stmt) if target_type == pl.LazyFrame else pdf.sql(stmt).collect()

    def to_arrow(self, obj: PolarsTypes) -> pa.Table:
        if isinstance(obj, pl.LazyFrame):
            return obj.collect().to_arrow()
        else:
            return obj.to_arrow()

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return (pl.LazyFrame, pl.DataFrame)


class IcebergDaftTypeHandler(IcebergBaseTypeHandler[da.DataFrame]):
    """Type handler that converts data between Iceberg tables and polars DataFrames"""

    def to_data_frame(
        self, table: ibt.Table, table_slice: TableSlice, target_type: Type[da.DataFrame]
    ) -> da.DataFrame:
        selected_fields: str = (
            ",".join(table_slice.columns) if table_slice.columns is not None else "*"
        )
        row_filter: str | None = None
        if table_slice.partition_dimensions:
            expressions = DagsterPartitionToDaftSqlPredicateMapper(
                partition_dimensions=table_slice.partition_dimensions,
                table_schema=table.schema(),
                table_partition_spec=table.spec(),
            ).partition_dimensions_to_filters()
            row_filter = " AND ".join(expressions)

        ddf = table.to_daft()  # type: ignore # noqa

        stmt = f"SELECT {selected_fields} FROM ddf"
        if row_filter is not None:
            stmt += f"\nWHERE {row_filter}"

        return da.sql(stmt)

    def to_arrow(self, obj: da.DataFrame) -> pa.Table:
        return obj.to_arrow()

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return [da.DataFrame]
