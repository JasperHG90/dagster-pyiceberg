from typing import Any, Dict

from dagster import Config


class IcebergCatalogConfig(Config):

    properties: Dict[str, Any]
