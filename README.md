# (Under development) Dagster-PyIceberg

Dagster IO manager for managing [PyIceberg](https://github.com/apache/iceberg-python) tables.

This implementation is based on the [dagster-deltalake](https://github.com/dagster-io/dagster/tree/master/python_modules/libraries/dagster-deltalake) IO manager.

## Usage

See 'examples' directory.

## Limitations

### When using partitioned assets

There currently is no internal retry mechanism when concurrent operations attempt to write to the commit log at the same time (this is not supported by PyIceberg). See e.g. [this issue](https://github.com/apache/iceberg-python/issues/269) and [this issue](https://github.com/apache/iceberg-python/issues/1084).

This means that, when using partitioned assets, you can see some partitions failing because another process was not finished writing to the commit log. A workaround has not yet been implemented for this.

### Implemented catalog backends

The following catalog backends are currently implemented.

- sql
- rest

### Implemented engines

The following engines are currently implemented.

- arrow
