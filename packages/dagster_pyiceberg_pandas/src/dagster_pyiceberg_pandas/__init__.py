# from dagster._core.libraries import DagsterLibraryRegistry
from dagster_pyiceberg_pandas.pyiceberg_pandas_type_handler import (
    IcebergPandasIOManager as IcebergPandasIOManager,
)
from dagster_pyiceberg_pandas.pyiceberg_pandas_type_handler import (
    IcebergPandasTypeHandler as IcebergPandasTypeHandler,
)
from dagster_pyiceberg_pandas.version import __version__ as __version__

# DagsterLibraryRegistry.register("dagster-pyiceberg-pandas", __version__)
