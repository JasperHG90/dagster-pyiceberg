import daft as da
import pandas as pd
from dagster import Definitions, asset

from dagster_pyiceberg.config import IcebergCatalogConfig
from dagster_pyiceberg.io_manager.daft import IcebergDaftIOManager

CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/examples/select_columns/catalog.db"
CATALOG_WAREHOUSE = (
    "file:///home/vscode/workspace/.tmp/examples/select_columns/warehouse"
)


resources = {
    "io_manager": IcebergDaftIOManager(
        name="test",
        config=IcebergCatalogConfig(
            properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
        ),
        schema="dagster",
    )
}


@asset
def iris_dataset() -> da.DataFrame:
    return da.from_pandas(
        pd.read_csv(
            "https://docs.dagster.io/assets/iris.csv",
            names=[
                "sepal_length_cm",
                "sepal_width_cm",
                "petal_length_cm",
                "petal_width_cm",
                "species",
            ],
        )
    )


defs = Definitions(assets=[iris_dataset], resources=resources)
