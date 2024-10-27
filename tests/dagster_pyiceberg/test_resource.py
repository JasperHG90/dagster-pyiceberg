from typing import Dict

import pyarrow as pa
import pytest
from dagster import asset, materialize
from dagster_pyiceberg import IcebergSqlCatalogConfig, IcebergTableResource
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table


@pytest.fixture(scope="module")
def table_name() -> str:
    return "resource_data"


@pytest.fixture(scope="function")
def table_identifier(namespace: str, table_name: str) -> str:
    return f"{namespace}.{table_name}"


@pytest.fixture(scope="function", autouse=True)
def create_table_in_catalog(
    catalog: SqlCatalog, table_identifier: str, data_schema: pa.Schema
):
    catalog.create_table(table_identifier, data_schema)


@pytest.fixture(scope="function", autouse=True)
def iceberg_table(
    create_table_in_catalog, catalog: SqlCatalog, table_identifier: str
) -> Table:
    return catalog.load_table(table_identifier)


@pytest.fixture(scope="function", autouse=True)
def append_data_to_table(iceberg_table: Table, data: pa.Table):
    iceberg_table.append(df=data)


@pytest.fixture(scope="function")
def pyiceberg_table_resource(
    catalog_name: str,
    namespace: str,
    table_name: str,
    catalog_config_properties: Dict[str, str],
) -> IcebergTableResource:
    return IcebergTableResource(
        name=catalog_name,
        config=IcebergSqlCatalogConfig(properties=catalog_config_properties),
        schema=namespace,
        table=table_name,
    )


def test_resource(
    pyiceberg_table_resource: IcebergTableResource,
    data: pa.Table,
):

    @asset
    def read_table(pyiceberg_table: IcebergTableResource):
        table_ = pyiceberg_table.load().scan().to_arrow()
        assert (
            table_.schema.to_string()
            == "timestamp: timestamp[us]\ncategory: large_string\nvalue: double"
        )
        assert table_.shape == (1440, 3)

    materialize(
        [read_table],
        resources={"pyiceberg_table": pyiceberg_table_resource},
    )


# def test_resource_versioned(tmp_path):
#     data = pa.table(
#         {
#             "a": pa.array([1, 2, 3], type=pa.int32()),
#             "b": pa.array([5, 6, 7], type=pa.int32()),
#         }
#     )

#     @asset
#     def create_table(delta_table: DeltaTableResource):
#         write_deltalake(
#             delta_table.url, data, storage_options=delta_table.storage_options.str_dict()
#         )
#         write_deltalake(
#             delta_table.url,
#             data,
#             storage_options=delta_table.storage_options.str_dict(),
#             mode="append",
#         )

#     @asset
#     def read_table(delta_table: DeltaTableResource):
#         res = delta_table.load().to_pyarrow_table()
#         assert res.equals(data)

#     materialize(
#         [create_table, read_table],
#         resources={
#             "delta_table": DeltaTableResource(
#                 url=os.path.join(tmp_path, "table"), storage_options=LocalConfig(), version=0
#             )
#         },
#     )


# @pytest.mark.parametrize(
#     "config",
#     [
#         LocalConfig(),
#         AzureConfig(account_name="test", use_azure_cli=True),
#         GcsConfig(bucket="test"),
#         S3Config(bucket="test"),
#         ClientConfig(timeout=1),
#     ],
# )
# def test_config_classes_are_string_dicts(tmp_path, config):
#     data = pa.table(
#         {
#             "a": pa.array([1, 2, 3], type=pa.int32()),
#             "b": pa.array([5, 6, 7], type=pa.int32()),
#         }
#     )
#     config_dict = config.str_dict()
#     assert all([isinstance(val, str) for val in config_dict.values()])
#     # passing the config_dict to write_deltalake validates that all dict values are str.
#     # it will throw an exception if there are other value types
#     write_deltalake(os.path.join(tmp_path, "table"), data, storage_options=config_dict)
