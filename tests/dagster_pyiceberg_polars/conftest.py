from typing import Dict

import pytest
from pyiceberg.catalog.sql import SqlCatalog


@pytest.fixture(scope="session", autouse=True)
def warehouse_path(tmp_path_factory) -> str:
    dir_ = tmp_path_factory.mktemp("warehouse")
    return str(dir_.resolve())


@pytest.fixture(scope="session")
def catalog_config_properties(warehouse_path: str) -> Dict[str, str]:
    return {
        "uri": f"sqlite:///{str(warehouse_path)}/pyiceberg_catalog.db",
        "warehouse": f"file://{str(warehouse_path)}",
    }


@pytest.fixture(scope="session")
def catalog_name() -> str:
    return "default"


@pytest.fixture(scope="session", autouse=True)
def catalog(catalog_name: str, catalog_config_properties: Dict[str, str]) -> SqlCatalog:
    return SqlCatalog(
        catalog_name,
        **catalog_config_properties,
    )


@pytest.fixture(scope="session", autouse=True)
def namespace(catalog: SqlCatalog) -> str:
    catalog.create_namespace("pytest")
    return "pytest"
