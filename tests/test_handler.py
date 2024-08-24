from pyiceberg.table import Table


def test_this(table: Table):
    table.scan().to_arrow()
