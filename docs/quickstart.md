# Quickstart

## Step 1: Defining the I/O manager

To use dagster-pyiceberg as an I/O manager, you add it to your `Definition`:

```python
from dagster import Definitions
from dagster_pyiceberg import IcebergPyarrowIOManager, IcebergSqlCatalogConfig

CATALOG_URI = "sqlite:////home/vscode/workspace/.tmp/dag/warehouse/catalog.db"
CATALOG_WAREHOUSE = "file:///home/vscode/workspace/.tmp/dag/warehouse"


resources = {
    "io_manager": IcebergPyarrowIOManager(
        name="test",
        config=IcebergSqlCatalogConfig(
            properties={"uri": CATALOG_URI, "warehouse": CATALOG_WAREHOUSE}
        ),
        schema="dagster",
    )
}


defs = Definitions(
    assets=[iris_dataset],
    resources=resources
)
```

> 💡 PyIceberg requires a catalog backend. A SQLite catalog is used here for illustrative purposes. Do not use this in a production setting.

## Step 2: Store a Dagster asset as a PyIceberg table

```python
import pandas as pd

from dagster import asset


@asset
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
```

## Step 3: Load PyIceberg tables in downstream assets

Dagster and the I/O manager allow you to load the data stored in Iceberg tables into downstream assets:

```python
import pandas as pd

from dagster import asset

# this example uses the iris_dataset asset from Step 2

@asset
def iris_cleaned(iris_dataset: pd.DataFrame) -> pd.DataFrame:
    return iris_dataset.dropna().drop_duplicates()
```
