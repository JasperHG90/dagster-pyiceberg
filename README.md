# (Under development) Dagster-PyIceberg

Dagster IO manager for managing [PyIceberg](https://github.com/apache/iceberg-python) tables.

This implementation is based on the [dagster-deltalake](https://github.com/dagster-io/dagster/tree/master/python_modules/libraries/dagster-deltalake) IO manager.

## Usage

See 'examples' directory.

## Limitations

### When using partitioned assets

PyIceberg cannot write concurrent commit logs. (Issue link).

### Implemented catalog backends

- sql
- rest

### Implemented engines

- arrow
