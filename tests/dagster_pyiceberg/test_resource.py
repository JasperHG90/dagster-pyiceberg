import pyarrow as pa
import pytest
from dagster import asset, materialize
from dagster_pyiceberg import IcebergSqlCatalogConfig, PyIcebergTableResource
from pyiceberg.catalog.sql import SqlCatalog


@pytest.fixture(scope="function")
def resource(tmp_path) -> PyIcebergTableResource:
    return PyIcebergTableResource(
        name="default",
        config=IcebergSqlCatalogConfig(
            properties={
                "uri": f"sqlite:///{str(tmp_path)}/pyiceberg_catalog.db",
                "warehouse": f"file://{str(tmp_path)}",
            }
        ),
        schema="resource",
        table="data",
    )


@pytest.fixture(autouse=True)
def sql_catalog_resource(resource: PyIcebergTableResource):
    return SqlCatalog(
        name="default",
        **resource.config.properties,  # NB: must match name in IO manager
    )


@pytest.fixture(autouse=True)
def create_schema_resource(
    sql_catalog_resource: SqlCatalog, resource: PyIcebergTableResource
):
    sql_catalog_resource.create_namespace("resource")


@pytest.fixture(autouse=True)
def create_catalog_table_resource(
    sql_catalog_resource: SqlCatalog, create_schema_resource, data: pa.Table
):
    sql_catalog_resource.create_table("resource.data", data.schema)
    table_ = sql_catalog_resource.load_table("resource.data")
    table_.append(data)


def test_resource(
    tmp_path,
    create_catalog_table_resource,
    resource: PyIcebergTableResource,
    data: pa.Table,
):
    data = data

    @asset
    def read_table(pyiceberg_table: PyIcebergTableResource):
        res = pyiceberg_table.load()
        _ = res.scan().to_arrow()

    materialize(
        [read_table],
        resources={"pyiceberg_table": resource},
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
