import datetime as dt
from typing import Dict, List, Mapping, Sequence, Union, cast  # noqa

from dagster import (
    AssetKey,
    InputContext,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    OutputContext,
    TimeWindowPartitionsDefinition,
)
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import (
    DbIOManager,
    TablePartitionDimension,
    TableSlice,
)
from dagster_pyiceberg._utils.transforms import date_diff
from pendulum import instance as pdi


class MultiTimePartitionsChecker:

    def __init__(self, partitions: List[TimeWindow]):
        self._partitions = partitions

    @property
    def start(self) -> dt.datetime:
        return min([w.start for w in self._partitions])

    @property
    def end(self) -> dt.datetime:
        return max([w.end for w in self._partitions])

    @property
    def hourly_deltas(self) -> List[int]:
        deltas = [date_diff(w.start, w.end).in_hours() for w in self._partitions]
        if len(set(deltas)) != 1:
            raise ValueError(
                "TimeWindowPartitionsDefinition must have the same delta from start to end"
            )
        return deltas

    @property
    def hourly_delta(self) -> int:
        try:
            return next(iter(set(self.hourly_deltas)))
        except StopIteration:
            raise ValueError(
                "TimeWindowPartitionsDefinition must have at least one partition"
            )

    def is_consecutive(self):
        return (
            True
            if len(
                set(
                    [
                        pdi(self.start).add(hours=self.hourly_delta * i)
                        for i in range(date_diff(self.start, self.end).in_days() + 1)
                    ]
                )
                - set([pdi(d.start) for d in self._partitions])
            )
            == 1
            else False
        )


def generate_multi_partitions_dimension(
    asset_partition_keys: Sequence[str],
    asset_partitions_def: MultiPartitionsDefinition,
    partition_expr: Mapping[str, str],
    asset_key: AssetKey,
) -> List[TablePartitionDimension]:
    partition_dimensions: List[TablePartitionDimension] = []
    multi_partition_key_mappings = [
        cast(MultiPartitionKey, partition_key).keys_by_dimension
        for partition_key in asset_partition_keys
    ]
    for part in asset_partitions_def.partitions_defs:
        partitions: List[Union[TimeWindow, str]] = []
        for multi_partition_key_mapping in multi_partition_key_mappings:
            partition_key = multi_partition_key_mapping[part.name]
            if isinstance(part.partitions_def, TimeWindowPartitionsDefinition):
                partitions.append(
                    part.partitions_def.time_window_for_partition_key(partition_key)
                )
            else:
                partitions.append(partition_key)

        partition_expr_str = partition_expr.get(part.name)
        if partition_expr is None:
            raise ValueError(
                f"Asset '{asset_key}' has partition {part.name}, but the"
                f" 'partition_expr' metadata does not contain a {part.name} entry,"
                " so we don't know what column to filter it on. Specify which"
                " column of the database contains data for the"
                f" {part.name} partition."
            )
        partitions_: TimeWindow | Sequence[str]
        if all(isinstance(partition, TimeWindow) for partition in partitions):
            checker = MultiTimePartitionsChecker(
                partitions=cast(List[TimeWindow], partitions)
            )
            if not checker.is_consecutive():
                raise ValueError("Dates are not consecutive.")
            partitions_ = TimeWindow(
                start=checker.start,
                end=checker.end,
            )
        elif all(isinstance(partition, str) for partition in partitions):
            partitions_ = list(set(partitions))
        else:
            raise ValueError("Unknown partition type")
        partition_dimensions.append(
            TablePartitionDimension(
                partition_expr=cast(str, partition_expr_str), partitions=partitions_
            )
        )
    return partition_dimensions


def generate_single_partition_dimension(
    partition_expr: str,
    asset_partition_keys: Sequence[str],
    asset_partitions_time_window: TimeWindow | None,
) -> TablePartitionDimension:
    partition_dimension: TablePartitionDimension
    if isinstance(asset_partitions_time_window, TimeWindow):
        partition_dimension = TablePartitionDimension(
            partition_expr=partition_expr,
            partitions=(asset_partitions_time_window if asset_partition_keys else []),
        )
    else:
        partition_dimension = TablePartitionDimension(
            partition_expr=partition_expr,
            partitions=asset_partition_keys,
        )
    return partition_dimension


class CustomDbIOManager(DbIOManager):
    """Works exactly like the DbIOManager, but overrides the _get_table_slice method
    to allow user to pass multiple partitions to select. This happens e.g. when a user makes
    a mapping from partition A to partition B, where A is partitioned on two dimensions and
    B is partitioned on only one dimension.

        See:
            - Issue: <https://github.com/dagster-io/dagster/issues/17838>
            - Open PR: <https://github.com/dagster-io/dagster/pull/20400>
    """

    def _get_schema(
        self,
        context: Union[OutputContext, InputContext],
        output_context_metadata: Dict[str, str],
    ) -> str:
        asset_key_path = context.asset_key.path
        # schema order of precedence: metadata, I/O manager 'schema' config, key_prefix
        if output_context_metadata.get("schema"):
            schema = cast(str, output_context_metadata["schema"])
        elif self._schema:
            schema = self._schema
        elif len(asset_key_path) > 1:
            schema = asset_key_path[-2]
        else:
            schema = "public"
        return schema

    def _get_table_slice(
        self, context: Union[OutputContext, InputContext], output_context: OutputContext
    ) -> TableSlice:
        output_context_metadata = output_context.definition_metadata or {}

        schema: str
        table: str
        partition_dimensions: List[TablePartitionDimension] = []
        if context.has_asset_key:
            table = context.asset_key.path[-1]
            schema = self._get_schema(context, output_context_metadata)

            if context.has_asset_partitions:
                partition_expr = output_context_metadata.get("partition_expr")
                if partition_expr is None:
                    raise ValueError(
                        f"Asset '{context.asset_key}' has partitions, but no 'partition_expr'"
                        " metadata value, so we don't know what column it's partitioned on. To"
                        " specify a column, set this metadata value. E.g."
                        ' @asset(metadata={"partition_expr": "your_partition_column"}).'
                    )
                if isinstance(context.asset_partitions_def, MultiPartitionsDefinition):
                    for partition_dimension in generate_multi_partitions_dimension(
                        asset_partition_keys=context.asset_partition_keys,
                        asset_partitions_def=context.asset_partitions_def,
                        partition_expr=cast(Mapping[str, str], partition_expr),
                        asset_key=context.asset_key,
                    ):
                        partition_dimensions.append(partition_dimension)
                else:
                    partition_dimensions.append(
                        generate_single_partition_dimension(
                            partition_expr=cast(str, partition_expr),
                            asset_partition_keys=context.asset_partition_keys,
                            asset_partitions_time_window=(
                                context.asset_partitions_time_window
                                if isinstance(
                                    context.asset_partitions_def,
                                    TimeWindowPartitionsDefinition,
                                )
                                else None
                            ),
                        )
                    )
        else:
            table = output_context.name
            if output_context_metadata.get("schema"):
                schema = cast(str, output_context_metadata["schema"])
            elif self._schema:
                schema = self._schema
            else:
                schema = "public"

        if isinstance(context, InputContext):
            1 == 1

        return TableSlice(
            table=table,
            schema=schema,
            database=self._database,
            partition_dimensions=partition_dimensions,
            columns=(context.definition_metadata or {}).get("columns"),
        )
