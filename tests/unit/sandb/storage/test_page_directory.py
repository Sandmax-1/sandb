import pytest

from sandb.storage.page_directory import PageDirectory, PageNotInDirectory, PagePointer
from sandb.storage.record import SchemaRecord
from sandb.storage.slotted_page import RecordNotInPage, SlottedPage
from sandb.storage.table_schema_page import TableSchemaPage
from tests.conftest import PAGE_SIZE_FOR_TESTS


def test_create_slotted_page(empty_page_directory: PageDirectory) -> None:
    """Test creating a default SlottedPage using create_page."""
    page = empty_page_directory.create_record_page()
    assert isinstance(page, SlottedPage)
    assert page.page_id == 1


def test_create_table_schema_page(empty_page_directory: PageDirectory) -> None:
    """Test creating a TableSchemaPage using create_page."""
    schema_page = empty_page_directory.create_schema_page()
    assert isinstance(schema_page, TableSchemaPage)
    assert schema_page.page_id == 1


def test_create_page_increments_next_page_id(
    empty_page_directory: PageDirectory,
) -> None:
    """Test that create_page increments the next_page_id correctly."""
    initial_next_page_id = empty_page_directory.next_page_id
    empty_page_directory.create_record_page()
    empty_page_directory.create_record_page()
    assert empty_page_directory.next_page_id == initial_next_page_id + 2


def test_write_and_read_page_record_page(empty_page_directory: PageDirectory) -> None:
    """Test writing and reading a SlottedPage to disk."""
    page_directory = empty_page_directory
    page = page_directory.create_record_page()
    test_record_data = "test record data".encode("utf-8")
    page.add_record(test_record_data)

    page_directory.write_page(page)

    read_page = page_directory.read_page(page.page_id)

    assert read_page is not None
    assert isinstance(read_page, SlottedPage)
    assert read_page.page_id == page.page_id
    assert test_record_data == read_page.get_record(0)
    assert PagePointer(1, PAGE_SIZE_FOR_TESTS) in page_directory.page_pointers
    assert page_directory.next_page_id == 2


def test_write_and_read_page_table_schema_page(
    empty_page_directory: PageDirectory,
) -> None:
    """Test writing and reading a TableSchemaPage to disk."""
    page_directory = empty_page_directory
    schema_page = page_directory.create_schema_page()
    record = SchemaRecord(0, "table_1", [1, 0], ["str_col", "int_col"])
    schema_page.add_record(record)

    page_directory.write_schema_page(schema_page)

    read_page = TableSchemaPage(page_directory.read_page(schema_page.page_id))

    assert read_page is not None
    assert read_page.page_id == schema_page.page_id
    assert read_page.get_record(0) == record


def test_read_non_existent_page_raises_exception(
    empty_page_directory: PageDirectory,
) -> None:
    non_existent_page_id = 999

    with pytest.raises(PageNotInDirectory):
        empty_page_directory.read_page(non_existent_page_id)


def test_write_multiple_pages_and_read_back(
    empty_page_directory: PageDirectory,
) -> None:
    page_directory = empty_page_directory
    pages: list[SlottedPage] = []
    num_pages = 3
    for _ in range(num_pages):
        page = page_directory.create_record_page()
        test_data = f"data for page {page.page_id}".encode("utf-8")
        page.add_record(test_data)
        page_directory.write_page(page)
        pages.append(page)

    for written_page in pages:
        read_page = page_directory.read_page(written_page.page_id)
        assert read_page is not None
        assert read_page.page_id == written_page.page_id
        expected_data = f"data for page {written_page.page_id}".encode("utf-8")
        assert expected_data == read_page.get_record(0)


def test_update_existing_page(empty_page_directory: PageDirectory) -> None:
    """Test updating an existing page and reading the updated content."""
    page_directory = empty_page_directory
    page = page_directory.create_record_page()
    initial_data = b"initial data"
    updated_data = b"updated data"

    old_record_id = page.add_record(initial_data)
    page_directory.write_page(page)

    page.delete(0)
    record_id = page.add_record(updated_data)
    page_directory.write_page(page)

    read_page = page_directory.read_page(page.page_id)
    assert read_page is not None
    assert updated_data == page.get_record(record_id)
    with pytest.raises(RecordNotInPage):
        page.get_record(old_record_id)


def test_page_directory_persists_on_page_write(
    empty_page_directory: PageDirectory,
) -> None:
    page_directory_original = empty_page_directory
    page = page_directory_original.create_record_page()
    test_data = b"data to test directory persistence"
    record_id = page.add_record(test_data)
    page_directory_original.write_page(page)

    page_directory_reloaded = PageDirectory.from_file(
        file_path=page_directory_original.file_path,
        page_directory_offset=0,
        page_directory_size_bytes=PAGE_SIZE_FOR_TESTS,
    )

    read_page = page_directory_reloaded.read_page(page.page_id)

    assert read_page is not None
    assert read_page.page_id == page.page_id
    assert test_data == read_page.get_record(record_id)
