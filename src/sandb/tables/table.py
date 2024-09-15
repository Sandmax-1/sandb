import json
from typing import Any, Iterable

from sandb.tables.metadata import TableMetadata


class TableExistsError(Exception):
    ...


class RowTypeError(Exception):
    """Error to raise when trying to insert a row to  table with the wrong dtypes"""

    ...


def create(metadata: TableMetadata) -> None:
    """
    Creates a "table" folder. Within this folder we have two files.
    A json file called metadata.json: this will store the contents of
    the metadata TableMetadata object and tell us how to access the other file.
    The other file is called data.csv. It is a csv that we can get the type info
    for from the metadata file. Each row in the csv will correspond to a row in
    the table.

    Args:
        metadata (TableMetadata): Contains all the metadata for the given table.
        I.e. name, columns, datatypes, e.t.c

    Raises:
        TableExistsError: Raised if we are trying to create a table that already exists.
                          i.e. a folder of metadata.name already exists.
    """
    table_path = metadata.location / metadata.name
    metadata_path = table_path / "metadata.json"
    data_path = table_path / "data.csv"

    if table_path.exists():
        raise TableExistsError(f"Table already exists at location: {metadata.location}")

    table_path.mkdir()
    data_path.touch()

    with open(metadata_path, "w") as f:
        json.dump(metadata.model_dump_json(), f)


def write(row: Iterable[Any], table: TableMetadata) -> None:
    """
    Take a row of arbitrary data, ensure it has the correct shape and types,
    then save to file

    Args:
        row (tuple[Any, ...]): arbitrary tuple which we are trying to save
                               to the database
        table (TableMetadata): table metadata for the row we are trying to save.

    Raises:
        e: _description_
    """
    try:
        typed_row = tuple(
            dtype(row) for dtype, row in zip(table.dtypes, row, strict=True)
        )
    except ValueError:
        raise RowTypeError(f"Failed to write row {row} due to type mismatch.", row)

    with open(table.data_path(), "a") as f:
        f.write(str(typed_row)[1:-1] + "\n")
