from typing import List, Mapping, Union, cast  # noqa

import pendulum
from dagster import (
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


class CustomDbIOManager(DbIOManager):
    """Works exactly like the DbIOManager, but overrides the _get_table_slice method
    to allow user to pass multiple partitions to select. This happens e.g. when a user makes
    a mapping from partition A to partition B, where A is partitioned on two dimensions and
    B is partitioned on only one dimension.

        See:
            - Issue: <https://github.com/dagster-io/dagster/issues/17838>
            - Open PR: <https://github.com/dagster-io/dagster/pull/20400>
    """

    def _get_table_slice(
        self, context: Union[OutputContext, InputContext], output_context: OutputContext
    ) -> TableSlice:
        output_context_metadata = output_context.definition_metadata or {}

        schema: str
        table: str
        partition_dimensions: List[TablePartitionDimension] = []
        if context.has_asset_key:
            asset_key_path = context.asset_key.path
            table = asset_key_path[-1]
            # schema order of precedence: metadata, I/O manager 'schema' config, key_prefix
            if output_context_metadata.get("schema"):
                schema = cast(str, output_context_metadata["schema"])
            elif self._schema:
                schema = self._schema
            elif len(asset_key_path) > 1:
                schema = asset_key_path[-2]
            else:
                schema = "public"

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
                    multi_partition_key_mappings = [
                        cast(MultiPartitionKey, partition_key).keys_by_dimension
                        for partition_key in context.asset_partition_keys
                    ]
                    for part in context.asset_partitions_def.partitions_defs:
                        partitions: List[Union[TimeWindow, str]] = []
                        for multi_partition_key_mapping in multi_partition_key_mappings:
                            partition_key = multi_partition_key_mapping[part.name]
                            if isinstance(
                                part.partitions_def, TimeWindowPartitionsDefinition
                            ):
                                partitions.append(
                                    part.partitions_def.time_window_for_partition_key(
                                        partition_key
                                    )
                                )
                            else:
                                partitions.append(partition_key)

                        partition_expr_str = cast(
                            Mapping[str, str], partition_expr
                        ).get(part.name)
                        if partition_expr is None:
                            raise ValueError(
                                f"Asset '{context.asset_key}' has partition {part.name}, but the"
                                f" 'partition_expr' metadata does not contain a {part.name} entry,"
                                " so we don't know what column to filter it on. Specify which"
                                " column of the database contains data for the"
                                f" {part.name} partition."
                            )
                        if all(
                            isinstance(partition, TimeWindow)
                            for partition in partitions
                        ):
                            partitions_ = cast(List[TimeWindow], partitions)
                            start = min([w.start for w in partitions_])
                            end = max([w.end for w in partitions_])
                            deltas = [
                                date_diff(w.start, w.end).in_hours()
                                for w in partitions_
                            ]
                            if len(set(deltas)) != 1:
                                raise ValueError(
                                    "TimeWindowPartitionsDefinition must have the same delta from start to end"
                                )
                            dates_passed = [
                                pendulum.instance(d.start) for d in partitions_
                            ]
                            dates_generated = [pendulum.instance(start)] + [
                                pendulum.instance(start).add(hours=deltas[0] * i)
                                for i in range(
                                    (
                                        pendulum.instance(end)
                                        - pendulum.instance(start)
                                    ).in_days()
                                    + 1
                                )
                            ]
                            if len(set(dates_generated) - set(dates_passed)) != 1:
                                raise ValueError("Dates are not consecutive.")
                            partition_dimensions.append(
                                TablePartitionDimension(
                                    partition_expr=cast(str, partition_expr_str),
                                    partitions=TimeWindow(
                                        start=min([w.start for w in partitions_]),
                                        end=max([w.end for w in partitions_]),
                                    ),
                                )
                            )
                        elif all(
                            isinstance(partition, str) for partition in partitions
                        ):
                            partitions_ = cast(List[str], partitions)
                            partition_dimensions.append(
                                TablePartitionDimension(
                                    partition_expr=cast(str, partition_expr_str),
                                    partitions=list(set(partitions_)),
                                )
                            )
                        else:
                            raise ValueError("Unknown partition type")
                elif isinstance(
                    context.asset_partitions_def, TimeWindowPartitionsDefinition
                ):
                    partition_dimensions.append(
                        TablePartitionDimension(
                            partition_expr=cast(str, partition_expr),
                            partitions=(
                                context.asset_partitions_time_window
                                if context.asset_partition_keys
                                else []
                            ),
                        )
                    )
                else:
                    partition_dimensions.append(
                        TablePartitionDimension(
                            partition_expr=cast(str, partition_expr),
                            partitions=context.asset_partition_keys,
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

        return TableSlice(
            table=table,
            schema=schema,
            database=self._database,
            partition_dimensions=partition_dimensions,
            columns=(context.definition_metadata or {}).get("columns"),
        )
