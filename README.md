# (Under development) Dagster-PyIceberg

Dagster IO manager for managing [PyIceberg](https://github.com/apache/iceberg-python) tables.

This implementation is based on the [dagster-deltalake](https://github.com/dagster-io/dagster/tree/master/python_modules/libraries/dagster-deltalake) IO manager.

## Usage

This library allows you to read from and write to Iceberg tables using PyIceberg.

You need to configure an Iceberg Catalog backend for this. See the [PyIceberg documentation](https://py.iceberg.apache.org/configuration/#catalogs) for more information.

Then, you can define the IO manager resource as follows:

```python
from dagster_pyiceberg import IcebergPyarrowIOManager, IcebergSqlCatalogConfig

CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/dag/warehouse/catalog.db"
CATALOG_WAREHOUSE = "file:///home/vscode/workspace/.tmp/dag/warehouse"

resources = {
    "io_manager": IcebergPyarrowIOManager(
        name="test",
        config=IcebergSqlCatalogConfig(
            properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
        ),
        schema="dagster",
    )
}
```

You can also use the IO manager with partitioned assets:

```python
from dagster import DailyPartitionsDefinition, Definitions, asset

partition = DailyPartitionsDefinition(
    start_date=dt.datetime(2024, 10, 1, 0, tzinfo=dt.timezone.utc),
    end_date=dt.datetime(2024, 10, 30, 0, tzinfo=dt.timezone.utc),
)


@asset(
    partitions_def=partition,
    metadata={"partition_expr": "date"},
)
def asset_1():
    data = {
        "date": [dt.datetime(2024, 10, i + 1, 0) for i in range(20)],
        "values": np.random.normal(0, 1, 20).tolist(),
    }
    return pa.Table.from_pydict(data)
```

For full examples, see 'examples' directory.

## Limitations

The table below shows which PyIceberg features are currently available.

| Feature | Supported | Link | Comment |
|---|---|---|---|
| Add existing files | ❌ | https://py.iceberg.apache.org/api/#add-files | Useful for existing partitions that users don't want to re-materialize/re-compute. |
| Schema evolution | ✅ | https://py.iceberg.apache.org/api/#schema-evolution | More complicated than e.g. delta lake since updates require diffing input table with existing Iceberg table. This is implemented by checking the schema of incoming data, dropping any columns that no longer exist in the data schema, and then using the `union_by_name()` method to merge the current schema with the table schema. Current implementation has a chance of creating a race condition when e.g. partition A tries to write to a table that has not yet processed a schema update. Should be covered by retrying when writing. |
| Sort order | ❌ | https://shorturl.at/TycZN | Currently not supported since there's no way to update them after a table has been created. For future reference: these can be partitions but that's not necessary. Also, they require a transform. Easiest thing to do is to allow end-users to set this in metadata. |
| PyIceberg commit retries | ✅ | https://github.com/apache/iceberg-python/pull/330 https://github.com/apache/iceberg-python/issues/269 | PR to add this to PyIceberg is open. Will probably be merged for an upcoming release. Added a custom retry function using Tenacity for the time being. |
| Partition evolution | ✅ | https://py.iceberg.apache.org/api/#partition-evolution | Create, Update, Delete partitions by updating the Dagster partitions definition. |
| Table properties | ✅ | https://py.iceberg.apache.org/api/#table-properties | Added as metadata on an asset. NB: config options are not checked explicitly because users can add any key-value pair to a table. Available properties [here](https://py.iceberg.apache.org/configuration/#tables). |
| Snapshot properties | ✅ | https://py.iceberg.apache.org/api/#snapshot-properties | Useful for correlating Dagster runs to snapshots by adding tags to snapshot. Not configurable by end-user. |
|

### Supported catalog backends

The following catalog backends are currently supported.

- sql
- rest

### Implemented engines

The following engines are currently implemented.

- arrow
- pandas

## Development

1. Clone repo
2. Set up the devcontainer
3. Run `just s` to install dependencies

## To do

- Add additional configuration options
  + Partition update error or update DONE
  + Sort ordering
- Examples:
  + Add cmd for running examples
  + Remove retry policy since we have fn to overwrite with retries
