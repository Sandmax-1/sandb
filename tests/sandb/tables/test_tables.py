import json
from typing import Any

import pytest

from sandb.tables.metadata import TableMetadata
from sandb.tables.table import RowTypeError, TableExistsError, create, read, write


def test_create_happy_path(test_table_metadata: TableMetadata) -> None:
    create(test_table_metadata)

    with open(
        test_table_metadata.location / test_table_metadata.name / "metadata.json"
    ) as f:
        saved_metadata = json.load(f)

    assert saved_metadata == test_table_metadata.model_dump_json()


def test_create_table_folder_already_exists(test_table_metadata: TableMetadata) -> None:
    (test_table_metadata.location / test_table_metadata.name).mkdir()

    with pytest.raises(TableExistsError):
        create(test_table_metadata)


# Helper function to read the data file for verification
def read_data_file(table: TableMetadata) -> list[str]:
    with open(table.data_path()) as f:
        return f.readlines()


def test_write_happy_path(test_table_metadata: TableMetadata) -> None:
    create(test_table_metadata)
    row = ("John", 30)

    write(row, test_table_metadata)

    data = read_data_file(test_table_metadata)
    assert len(data) == 1
    assert data[0].strip() == "John, 30"


# Test for TypeError when passing invalid data type
def test_write_invalid_type(test_table_metadata: TableMetadata) -> None:
    row = (
        "John",
        "InvalidAge",
    )

    with pytest.raises(RowTypeError):
        write(row, test_table_metadata)


def test_write_append_rows(test_table_metadata: TableMetadata) -> None:
    create(test_table_metadata)
    row1 = ("Alice", 25)
    row2 = ("Bob", 40)

    write(row1, test_table_metadata)
    write(row2, test_table_metadata)

    data = read_data_file(test_table_metadata)
    assert len(data) == 2
    assert data[0].strip() == "Alice, 25"
    assert data[1].strip() == "Bob, 40"


def test_write_incorrect_row_length(test_table_metadata: TableMetadata) -> None:
    row = ("Alice",)  # Row has only one element but two are expected

    with pytest.raises(RowTypeError):
        write(row, test_table_metadata)


@pytest.mark.parametrize(  # type: ignore
    argnames=["rows_to_insert", "column_to_query", "predicate", "expected"],
    argvalues=[
        ((("Alice", 10), ("Bob", 15)), "col_1", "Alice", [("Alice", 10)]),
        (
            (("Alice", 10), ("Bob", 15), ("Alice", 20)),
            "col_1",
            "Alice",
            [("Alice", 10), ("Alice", 20)],
        ),
        ((("Alice", 10), ("Bob", 15)), "col_1", "Chris", []),
    ],
    ids=["one matching row", "two matching rows", "no matching rows"],
)
def test_read(
    rows_to_insert: tuple[tuple[Any]],
    column_to_query: str,
    predicate: Any,
    expected: list[tuple[Any]],
    test_table_metadata: TableMetadata,
) -> None:
    create(test_table_metadata)
    for row in rows_to_insert:
        write(row, test_table_metadata)
    actual = read(column_to_query, predicate, test_table_metadata)

    assert actual == expected
