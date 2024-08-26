import sys
from typing import Any, Dict

from dagster import Config

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


class _IcebergCatalogBaseConfig(Config):

    name: str
    properties: Dict[str, Any]


class IcebergSqlCatalogConfig(_IcebergCatalogBaseConfig):

    type: Literal["sql"]


class IcebergRestCatalogConfig(_IcebergCatalogBaseConfig):

    type: Literal["rest"]
