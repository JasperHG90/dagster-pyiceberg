import pytest


@pytest.fixture(scope="session", autouse=True)
def warehouse_path(tmp_path_factory) -> str:
    dir_ = tmp_path_factory.mktemp("warehouse")
    return str(dir_.resolve())
