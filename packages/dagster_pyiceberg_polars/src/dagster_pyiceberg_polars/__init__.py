# from dagster._core.libraries import DagsterLibraryRegistry
from dagster_pyiceberg_polars.pyiceberg_polars_type_handler import (
    IcebergPolarsIOManager as IcebergPolarsIOManager,
)
from dagster_pyiceberg_polars.pyiceberg_polars_type_handler import (
    IcebergPolarsTypeHandler as IcebergPolarsTypeHandler,
)
from dagster_pyiceberg_polars.version import __version__ as __version__

# DagsterLibraryRegistry.register("dagster-pyiceberg-polars", __version__)
