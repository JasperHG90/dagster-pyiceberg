from dagster._core.libraries import DagsterLibraryRegistry

# from dagster_pyiceberg_pandas.pyiceberg_pandas_type_handler import (
#     DeltaLakePandasIOManager as DeltaLakePandasIOManager,
#     DeltaLakePandasTypeHandler as DeltaLakePandasTypeHandler,
# )
from dagster_pyiceberg_pandas.version import __version__

DagsterLibraryRegistry.register("dagster-pyiceberg-pandas", __version__)
