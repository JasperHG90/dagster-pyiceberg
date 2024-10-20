# (Under development) Dagster-PyIceberg

Dagster IO manager for managing [PyIceberg](https://github.com/apache/iceberg-python) tables.

This implementation is based on the [dagster-deltalake](https://github.com/dagster-io/dagster/tree/master/python_modules/libraries/dagster-deltalake) IO manager.

## Usage

See 'examples' directory.

## Limitations

The table below shows which PyIceberg features are currently available.

| Feature | Supported | Link | Comment |
|---|---|---|---|
| Adding files | ❌ | https://py.iceberg.apache.org/api/#add-files | Useful for existing partitions that users don't want to re-materialize/re-compute. |
| Schema evolution | ❌ | https://py.iceberg.apache.org/api/#schema-evolution | More complicated than e.g. delta lake since updates require diffing input table with existing Iceberg table. Approach should be similar to partition evolution. |
| Sort order | ❌ | https://shorturl.at/TycZN |  |
| PyIceberg commit retries | ✅ | https://github.com/apache/iceberg-python/pull/330 https://github.com/apache/iceberg-python/issues/269 | PR to add this to PyIceberg is open. Will probably be merged for an upcoming release. Added a custom retry function using Tenacity for the time being. |
| Partition evolution | ✅ | https://py.iceberg.apache.org/api/#partition-evolution | Create, Update, Delete |
| Table properties | ❌ | https://py.iceberg.apache.org/api/#table-properties | Can add this through metadata on the asset. |
| Snapshot properties | ❌ | https://py.iceberg.apache.org/api/#snapshot-properties | Useful for correlating Dagster runs to snapshots by adding tags to snapshot. |

### Implemented catalog backends

The following catalog backends are currently implemented.

- sql
- rest

### Implemented engines

The following engines are currently implemented.

- arrow
- pandas

## To do

- Add additional configuration options
  + Partition update error or update DONE
  + Sort ordering
- Examples:
  + Add cmd for running examples
  + Remove retry policy since we have fn to overwrite with retries
