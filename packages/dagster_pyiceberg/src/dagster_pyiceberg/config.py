import sys
from typing import Any, Dict

from dagster import Config

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


class _IcebergCatalogBaseConfig(Config):

    properties: Dict[str, Any]


class IcebergSqlCatalogConfig(_IcebergCatalogBaseConfig):

    type: Literal["sql"] = "sql"


class IcebergRestCatalogConfig(_IcebergCatalogBaseConfig):

    type: Literal["rest"] = "rest"


# Options (not yet covered)
#  Overwrite partition spec (or not)
#  Update schemas (or fail)

# Metadata (not yet covered)
#  Sort order (metadata)
#  Partition_expr mapping s.a. dagster-deltalake (metadata)
#  Transforms (metadata)
