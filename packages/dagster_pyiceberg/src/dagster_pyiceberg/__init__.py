from typing import Sequence

from dagster._core.libraries import DagsterLibraryRegistry
from dagster._core.storage.db_io_manager import DbTypeHandler
from dagster_pyiceberg.config import (
    IcebergRestCatalogConfig as IcebergRestCatalogConfig,
)
from dagster_pyiceberg.config import IcebergSqlCatalogConfig as IcebergSqlCatalogConfig
from dagster_pyiceberg.handler import (
    IcebergPyArrowTypeHandler as IcebergPyArrowTypeHandler,
)
from dagster_pyiceberg.io_manager import IcebergIOManager as IcebergIOManager
from dagster_pyiceberg.resource import IcebergTableResource as IcebergTableResource
from dagster_pyiceberg.version import __version__


class IcebergPyarrowIOManager(IcebergIOManager):
    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [IcebergPyArrowTypeHandler()]


DagsterLibraryRegistry.register("dagster-pyiceberg", __version__)
