from pathlib import Path
from struct import pack
from tempfile import TemporaryDirectory

from sandb.storage.database import Database
from sandb.storage.page_directory import PageDirectoryHeader, PagePointer
from sandb.storage.record import SchemaRecord
from sandb.storage.slotted_page import (
    SLOTTED_PAGE_HEADER_METADATA_SIZE,
    SlottedPage,
    SlottedPageHeader,
)


def test_database_with_page_directory_and_slotted_pages() -> None:
    page_size = 4096
    with TemporaryDirectory() as tmp:
        database = Database(
            file_path=Path(tmp) / "database",
            version="v0.0.1",
            page_size_bytes=page_size,
            page_directory_size_bytes=page_size,
        )

        page_directory = PageDirectoryHeader(
            page_directory_id=0,
            free_space_start=0,
            directory_size=database.page_directory_size_bytes,
            page_pointers=[
                PagePointer(
                    page_id=0,
                    page_start=database.page_directory_start
                    + database.page_directory_size_bytes,  # noqa
                )
            ],
        )

        schema_record = SchemaRecord(
            table_id=0,
            table_name="table_1",
            dtypes=[1, 0],
            col_names=["str_col", "int_col"],
        )

        schema_record_length = len(bytes(schema_record))

        schema_slot_offset = database.page_size_bytes - schema_record_length

        record = pack("<3si", "abc".encode("utf-8"), 10)

        slotted_page_header = SlottedPageHeader(
            page_id=0,
            free_space_start=SLOTTED_PAGE_HEADER_METADATA_SIZE,
            free_space_end=schema_slot_offset - len(record),
            slots=[],
        )

        slotted_page = SlottedPage(
            slotted_page_header,
            bytearray(bytes(slotted_page_header).ljust((page_size), b"\0")),
            page_size,
        )
        slotted_page.add_record(bytes(schema_record))
        slotted_page.add_record(record)

        with open(database.file_path, "wb") as f:
            f.write(bytes(database))
            f.write(bytes(page_directory))
            f.write(slotted_page.byte_str)

        with open(database.file_path, "rb") as f:
            loaded_database = Database.from_bytes(f.read(100), database.file_path)

            loaded_page_directory = database.page_directory

            f.seek(page_directory.page_pointers[0].page_start)
            loaded_slotted_page = SlottedPage.from_bytes(
                f.read(database.page_size_bytes)
            )

        assert database == loaded_database
        assert page_directory == loaded_page_directory
        assert slotted_page == loaded_slotted_page
