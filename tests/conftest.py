import pytest
from pytest import TempPathFactory
from sortedcontainers import SortedDict

from sandb.storage.record import SchemaRecord
from sandb.storage.slotted_page import (
    SLOTTED_PAGE_HEADER_METADATA_SIZE,
    SlottedPage,
    SlottedPageHeader,
)
from sandb.tables.metadata import Column, TableMetadata


@pytest.fixture  # type: ignore
def test_tree() -> SortedDict[str, int]:
    countries_dict = SortedDict(
        {
            "Bulgaria": 10,
            "Cyprus": 20,
            "Germany": 30,
            "Greenland": 40,
            "Hungary": 50,
            "Iceland": 60,
            "Ireland": 70,
            "Macedonia": 80,
            "Portugal": 90,
            "Sweden": 100,
        }
    )

    return countries_dict


@pytest.fixture  # type: ignore
def test_table_metadata(tmp_path_factory: TempPathFactory) -> TableMetadata:
    path = tmp_path_factory.mktemp("tables")
    return TableMetadata(
        name="test_table",
        columns=(Column(name="col_1", dtype=str), Column(name="col_2", dtype=int)),
        location=path,
    )


@pytest.fixture  # type: ignore
def schema_record() -> SchemaRecord:
    return SchemaRecord(
        table_id=0,
        table_name="table_1",
        dtypes=[1, 0],
        col_names=["str_col", "int_col"],
    )


@pytest.fixture(scope="function")  # type: ignore
def slotted_page(schema_record: SchemaRecord):
    """Fixture to set up a SlottedPage with initial records for testing."""
    header = SlottedPageHeader(
        page_id=0,
        free_space_start=SLOTTED_PAGE_HEADER_METADATA_SIZE,
        free_space_end=4096,
        slots=[],
    )

    slotted_page = SlottedPage(header=header, byte_str=bytearray(4096))

    slotted_page.add_record(bytes(schema_record))

    record1 = b"record_data_1"
    record2 = b"record_data_2_longer"
    record3 = b"record_data_3"

    slotted_page.add_record(record1)
    slotted_page.add_record(record2)
    slotted_page.add_record(record3)

    return slotted_page
