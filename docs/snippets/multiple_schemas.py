import pandas as pd
from dagster import Definitions, asset
from dagster_pyiceberg import IcebergSqlCatalogConfig
from dagster_pyiceberg_pandas import IcebergPandasIOManager

CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/catalog.db"
CATALOG_WAREHOUSE = "file:///home/vscode/workspace/.tmp/examples/warehouse"


resources = {
    "io_manager": IcebergPandasIOManager(
        name="test",
        config=IcebergSqlCatalogConfig(
            properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
        ),
        schema="dagster",
    )
}


@asset(key_prefix=["iris"])  # will be stored in "iris" schema
def iris_dataset() -> pd.DataFrame:
    return pd.read_csv(
        "https://docs.dagster.io/assets/iris.csv",
        names=[
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
            "species",
        ],
    )


@asset(key_prefix=["wine"])  # will be stored in "wine" schema
def wine_dataset() -> pd.DataFrame:
    return pd.read_csv(
        "https://gist.githubusercontent.com/tijptjik/9408623/raw/b237fa5848349a14a14e5d4107dc7897c21951f5/wine.csv",
        names=[
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
            "species",
        ],
    )


defs = Definitions(assets=[iris_dataset, wine_dataset], resources=resources)
