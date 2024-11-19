import pandas as pd
from dagster import Definitions, StaticPartitionsDefinition, asset

from dagster_pyiceberg.config import IcebergCatalogConfig
from dagster_pyiceberg.io_manager.pandas import IcebergPandasIOManager

CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/catalog.db"
CATALOG_WAREHOUSE = "file:///home/vscode/workspace/.tmp/examples/warehouse"


resources = {
    "io_manager": IcebergPandasIOManager(
        name="test",
        config=IcebergCatalogConfig(
            properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
        ),
        schema="dagster",
    )
}


@asset(
    partitions_def=StaticPartitionsDefinition(
        ["Iris-setosa", "Iris-virginica", "Iris-versicolor"]
    ),
    metadata={"partition_expr": "species"},
)
def iris_dataset_partitioned(context) -> pd.DataFrame:
    species = context.partition_key

    full_df = pd.read_csv(
        "https://docs.dagster.io/assets/iris.csv",
        names=[
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
            "species",
        ],
    )

    return full_df[full_df["species"] == species]


@asset
def iris_cleaned(iris_dataset_partitioned: pd.DataFrame):
    return iris_dataset_partitioned.dropna().drop_duplicates()


defs = Definitions(assets=[iris_dataset_partitioned, iris_cleaned], resources=resources)
