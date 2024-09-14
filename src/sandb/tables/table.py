import json

from sandb.tables.metadata import TableMetadata


class TableExistsError(Exception):
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
