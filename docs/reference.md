# dagster-pyiceberg integration reference

This reference page provides information for working with dagster-pyiceberg.

- [Selecting specific columns in a downstream asset](#selecting-specific-columns-in-a-downstream-asset)
- [Storing partitioned assets](#storing-partitioned-assets)
- [Storing tables in multiple schemas](#storing-tables-in-multiple-schemas)
- [Using the PyIceberg I/O manager with other I/O managers](#using-the-delta-lake-io-manager-with-other-io-managers)
- [Storing and loading PyArrow Tables, or Pandas and Polars DataFrames in PyIceberg](#storing-and-loading-pyarrow-tables-or-polars-dataframes-in-delta-lake)

---

## Selecting specific columns in a downstream asset

At times, you might prefer not to retrieve an entire table for a downstream asset. The PyIceberg I/O manager allows you to load specific columns by providing metadata related to the downstream asset.

```python title="docs/snippets/select_columns.py" linenums="1"
--8<-- "docs/snippets/select_columns.py"
```

In this example, we focus exclusively on the columns containing sepal data from the `iris_dataset` table. To select specific columns, we can include metadata in the input asset. This is done using the `metadata` parameter of the `AssetIn` that loads the `iris_dataset` asset within the `ins` parameter. We provide the key `columns` along with a list of the desired column names.

When Dagster materializes `sepal_data` and retrieves the `iris_dataset` asset via the PyIceberg I/O manager, it will only extract the `sepal_length_cm` and `sepal_width_cm` columns from the `iris/iris_dataset` table and deliver them to `sepal_data` as a Pandas DataFrame.

---

## Storing partitioned assets

The PyIceberg I/O manager facilitates the storage and retrieval of partitioned data. To effectively manage data in the Iceberg table, it is essential for the PyIceberg I/O manager to identify the column that specifies the partition boundaries. This information allows the I/O manager to formulate the appropriate queries for selecting or replacing data.

In the subsequent sections, we will outline how the I/O manager generates these queries for various partition types.

!!! info "Partition dimensions"

    For partitioning to function correctly, the partition dimension must correspond to one of the partition columns defined in the Iceberg table. Tables created through the I/O manager will be set up accordingly.

=== "Storing static partitioned assets"

    To save static partitioned assets in your Iceberg table, you need to set the `partition_expr` metadata on the asset. This informs the PyIceberg I/O manager which column holds the partition data:

    ```python title="docs/snippets/partitions_static.py" linenums="1"
    --8<-- "docs/snippets/partitions_static.py"
    ```

    Dagster uses the `partition_expr` metadata to create the necessary function parameters when retrieving the partition in the downstream asset. For static partitions, this is roughly equivalent to the following SQL query:

    ```sql
    SELECT *
    WHERE [partition_expr] in ([selected partitions])
    ```

    A partition must be specified when materializing the above assets, as explained in the [Materializing partitioned assets](/concepts/partitions-schedules-sensors/partitioning-assets#materializing-partitioned-assets) documentation. For instance, the query used to materialize the `Iris-setosa` partition of the assets would be:

    ```sql
    SELECT *
    WHERE species = 'Iris-setosa'
    ```

=== "Storing time-partitioned assets"

    Like static partitioned assets, you can specify `partition_expr` metadata on the asset to tell the PyIceberg I/O manager which column contains the partition data:


    ```python title="docs/snippets/partitions_time.py" linenums="1"
    --8<-- "docs/snippets/partitions_time.py"
    ```

    Dagster uses the `partition_expr` metadata to craft the `SELECT` statement when loading the correct partition in the downstream asset. When loading a dynamic partition, the following statement is used:

    ```sql
    SELECT *
    WHERE [partition_expr] = [partition_start]
    ```

    A partition must be selected when materializing the above assets, as described in the [Materializing partitioned assets](/concepts/partitions-schedules-sensors/partitioning-assets#materializing-partitioned-assets) documentation. The `[partition_start]` and `[partition_end]` bounds are of the form `YYYY-MM-DD HH:MM:SS`. In this example, the query when materializing the `2023-01-02` partition of the above assets would be:

    ```sql
    SELECT *
    WHERE time = '2023-01-02 00:00:00'
    ```

=== "Storing multi-partitioned assets"

    The PyIceberg I/O manager can also store data partitioned on multiple dimensions. To do this, specify the column for each partition as a dictionary of `partition_expr` metadata:

    ```python title="docs/snippets/partitions_multiple.py" linenums="1"
    --8<-- "docs/snippets/partitions_multiple.py"
    ```

    Dagster uses the `partition_expr` metadata to craft the `SELECT` statement when loading the correct partition in a downstream asset. For multi-partitions, Dagster concatenates the `WHERE` statements described in the above sections to craft the correct `SELECT` statement.

    A partition must be selected when materializing the above assets, as described in the [Materializing partitioned assets](/concepts/partitions-schedules-sensors/partitioning-assets#materializing-partitioned-assets) documentation. For example, when materializing the `2023-01-02|Iris-setosa` partition of the above assets, the following query will be used:

    ```sql
    SELECT *
    WHERE species = 'Iris-setosa'
      AND time = '2023-01-02 00:00:00'
    ```

---

## Storing tables in multiple schemas

You may want to have different assets stored in different PyIceberg schemas. The PyIceberg I/O manager allows you to specify the schema in several ways.

If you want all of your assets to be stored in the same schema, you can specify the schema as configuration to the I/O manager, as we did in [Step 1](/integrations/deltalake/using-deltalake-with-dagster#step-1-configure-the-delta-lake-io-manager) of the [Using Dagster with PyIceberg tutorial](/integrations/deltalake/using-deltalake-with-dagster).

If you want to store assets in different schemas, you can specify the schema as part of the asset's key:

```python file=/integrations/deltalake/schema.py startafter=start_asset_key endbefore=end_asset_key

```

In this example, the `iris_dataset` asset will be stored in the `IRIS` schema, and the `daffodil_dataset` asset will be found in the `DAFFODIL` schema.

<Note>
  The two options for specifying schema are mutually exclusive. If you provide{" "}
  <code>schema</code> configuration to the I/O manager, you cannot also provide
  it via the asset key and vice versa. If no <code>schema</code> is provided,
  either from configuration or asset keys, the default schema{" "}
  <code>public</code> will be used.
</Note>

---

## Using the PyIceberg I/O manager with other I/O managers

You may have assets that you don't want to store in PyIceberg. You can provide an I/O manager to each asset using the `io_manager_key` parameter in the <PyObject object="asset" decorator /> decorator:

```python title="docs/snippets/multiple_io_managers.py" linenums="1"
--8<-- "docs/snippets/multiple_io_managers.py"
```

In this example:

- The `iris_dataset` asset uses the I/O manager bound to the key `warehouse_io_manager` and `iris_plots` uses the I/O manager bound to the key `blob_io_manager`
- In the <PyObject object="Definitions" /> object, we supply the I/O managers for those keys
- When the assets are materialized, the `iris_dataset` will be stored in PyIceberg, and `iris_plots` will be saved in Amazon S3

---

## Storing and loading PyArrow, Pandas, or Polars DataFrames with PyIceberg

The PyIceberg I/O manager also supports storing and loading PyArrow and Polars DataFrames.

### Storing and loading PyArrow Tables with PyIceberg

The `deltalake` package relies heavily on Apache Arrow for efficient data transfer, so PyArrow is natively supported.

You can use the `DeltaLakePyArrowIOManager` in a <PyObject object="Definitions" /> object as in [Step 1](/integrations/deltalake/using-deltalake-with-dagster#step-1-configure-the-delta-lake-io-manager) of the [Using Dagster with PyIceberg tutorial](/integrations/deltalake/using-deltalake-with-dagster).

```python title="docs/snippets/io_manager_pyarrow.py" linenums="1"
--8<-- "docs/snippets/io_manager_pyarrow.py"
```

### Storing and loading Pandas Tables with PyIceberg


### Storing and loading Polars Tables with PyIceberg
