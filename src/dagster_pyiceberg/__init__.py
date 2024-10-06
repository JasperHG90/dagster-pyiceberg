from typing import Sequence

from dagster._core.libraries import DagsterLibraryRegistry
from dagster._core.storage.db_io_manager import DbTypeHandler

from dagster_pyiceberg.config import IcebergSqlCatalogConfig  # noqa
from dagster_pyiceberg.handler import IcebergPyArrowTypeHandler
from dagster_pyiceberg.io_manager import BaseIcebergIOManager
from dagster_pyiceberg.version import __version__


class IcebergPyarrowIOManager(BaseIcebergIOManager):
    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [IcebergPyArrowTypeHandler()]


DagsterLibraryRegistry.register("dagster-pyiceberg", __version__)
