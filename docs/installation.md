# Installing dagster-pyiceberg

This library has alpha status. For a specific release, you can install it using:

```shell
pip install https://github.com/JasperHG90/dagster-pyiceberg/releases/download/v<VERSION>/dagster_pyiceberg-<VERSION>-py3-none-any.whl
```

Or e.g.:

```shell
uv add https://github.com/JasperHG90/dagster-pyiceberg/releases/download/v<VERSION>/dagster_pyiceberg-<VERSION>-py3-none-any.whl
```

You can find a list of versions / releases [here](https://github.com/JasperHG90/dagster-pyiceberg/releases).

!!! danger "PyIceberg dependency"

    This library depends on a development version of pyiceberg. As such, you should also install that library
    from git rather than PyPi in your project dependencies.

    ```shell
    # pip
    pip install git+https://github.com/apache/iceberg-python@0cebec4
    ```

    Or e.g.:

    ```shell
    # uv
    uv add git+https://github.com/apache/iceberg-python --rev 0cebec4
    ```

To install `dagster-pyiceberg-pandas` and `dagster-pyiceberg-polars`, you can use the same method as described above.
