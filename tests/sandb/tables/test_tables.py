import json
from typing import Any

import pytest

from sandb.tables.table import RowTypeError, Table, TableExistsError


def test_create_happy_path(test_table: Table) -> None:
    test_table.create()

    with open(test_table.metadata.metadata_path) as f:
        saved_metadata = json.load(f)

    assert saved_metadata == test_table.metadata.model_dump_json()


def test_create_table_folder_already_exists(test_table: Table) -> None:
    (test_table.metadata.location / test_table.metadata.name).mkdir()

    with pytest.raises(TableExistsError):
        test_table.create()


# Helper function to read the data file for verification
def read_data_file(table: Table) -> list[str]:
    with open(table.metadata.data_path) as f:
        return f.readlines()


def test_write_happy_path(test_table: Table) -> None:
    test_table.create()
    row = ("John", 30)

    test_table.write(row)

    data = read_data_file(test_table)
    assert len(data) == 1
    assert data[0].strip() == "John, 30"


# Test for TypeError when passing invalid data type
def test_write_invalid_type(test_table: Table) -> None:
    row = (
        "John",
        "InvalidAge",
    )

    with pytest.raises(RowTypeError):
        test_table.write(row)


def test_write_append_rows(test_table: Table) -> None:
    test_table.create()
    row1 = ("Alice", 25)
    row2 = ("Bob", 40)

    test_table.write(row1)
    test_table.write(row2)

    data = read_data_file(test_table)
    assert len(data) == 2
    assert data[0].strip() == "Alice, 25"
    assert data[1].strip() == "Bob, 40"


def test_write_incorrect_row_length(test_table: Table) -> None:
    row = ("Alice",)  # Row has only one element but two are expected

    with pytest.raises(RowTypeError):
        test_table.write(row)


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
    test_table: Table,
) -> None:
    test_table.create()
    for row in rows_to_insert:
        test_table.write(row)
    actual = test_table.read(column_to_query, predicate)

    assert actual == expected
