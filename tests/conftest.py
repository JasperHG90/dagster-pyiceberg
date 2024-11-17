import datetime as dt
import random
from typing import Dict, Iterator

import psycopg2
import pyarrow as pa
import pytest
from pyiceberg.catalog import Catalog, load_catalog
from testcontainers.postgres import PostgresContainer

postgres = PostgresContainer("postgres:17-alpine")


@pytest.fixture(scope="session", autouse=True)
def setup(request: pytest.FixtureRequest) -> Iterator[PostgresContainer]:
    postgres.start()

    def remove_container():
        postgres.stop()

    request.addfinalizer(remove_container)

    yield postgres


@pytest.fixture(scope="session")
def postgres_connection(
    setup: PostgresContainer,
) -> Iterator[psycopg2.extensions.connection]:
    conn = psycopg2.connect(
        database=setup.dbname,
        port=setup.get_exposed_port(5432),
        host=setup.get_container_host_ip(),
        user=setup.username,
        password=setup.password,
    )
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def postgres_uri(setup: PostgresContainer) -> str:
    return setup.get_connection_url()


# NB: we truncate all iceberg tables before each test
#  that way, we don't have to worry about side effects
@pytest.fixture(scope="function", autouse=True)
def clean_iceberg_tables(postgres_connection: psycopg2.extensions.connection):
    with postgres_connection.cursor() as cur:
        cur.execute(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename LIKE 'iceberg%';"
        )
        for tbl in cur.fetchall():
            cur.execute(f"TRUNCATE TABLE {tbl[0]};")
    postgres_connection.commit()


# NB: recreated for every test
@pytest.fixture(scope="function", autouse=True)
def warehouse_path(tmp_path_factory: pytest.TempPathFactory) -> str:
    dir_ = tmp_path_factory.mktemp("warehouse")
    return str(dir_.resolve())


@pytest.fixture(scope="function")
def catalog_config_properties(warehouse_path: str, postgres_uri: str) -> Dict[str, str]:
    return {
        "uri": postgres_uri,
        "warehouse": f"file://{str(warehouse_path)}",
    }


@pytest.fixture(scope="session")
def catalog_name() -> str:
    return "default"


@pytest.fixture(scope="function")
def catalog(catalog_name: str, catalog_config_properties: Dict[str, str]) -> Catalog:
    return load_catalog(
        name=catalog_name,
        **catalog_config_properties,
    )


@pytest.fixture(scope="function", autouse=True)
def namespace(catalog: Catalog) -> str:
    catalog.create_namespace("pytest")
    return "pytest"


@pytest.fixture(scope="session")
def data() -> pa.Table:
    random.seed(876)
    N = 1440
    d = {
        "timestamp": pa.array(
            [
                dt.datetime(2023, 1, 1, 0, 0, 0) + dt.timedelta(minutes=i)
                for i in range(N)
            ]
        ),
        "category": pa.array([random.choice(["A", "B", "C"]) for _ in range(N)]),
        "value": pa.array(random.uniform(0, 1) for _ in range(N)),
    }
    return pa.Table.from_pydict(d)


@pytest.fixture(scope="session")
def data_schema(data: pa.Table) -> pa.Schema:
    return data.schema
