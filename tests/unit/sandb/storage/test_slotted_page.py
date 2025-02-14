from struct import pack

from sandb.storage.record import SchemaRecord
from sandb.storage.slotted_page import (
    SLOT_SIZE,
    SLOTTED_PAGE_HEADER_METADATA_SIZE,
    Slot,
    SlottedPage,
    SlottedPageHeader,
)


def test_slotted_page_header_serialisation() -> None:
    slotted_page_header = SlottedPageHeader(
        page_id=0,
        free_space_start=28,
        free_space_end=32,
        slots=[Slot(record_id=0, record_pointer=33, record_length=4)],
    )

    assert slotted_page_header == SlottedPageHeader.from_bytes(
        bytes(slotted_page_header)
    )


def test_slotted_page_add_record() -> None:
    page_size = 150
    schema_record = SchemaRecord(
        0, "table_1", dtypes=[1, 0], col_names=["str_col", "int_col"]
    )
    schema_record_bytes = bytes(schema_record)
    free_space_end = page_size - len(schema_record_bytes)

    header = SlottedPageHeader(
        page_id=0,
        free_space_start=SLOTTED_PAGE_HEADER_METADATA_SIZE + SLOT_SIZE,
        free_space_end=free_space_end,
        slots=[
            Slot(
                record_id=0,
                record_pointer=free_space_end,
                record_length=4,
            )
        ],
        next_row_id=1,
    )

    slotted_page = SlottedPage(
        header=header,
        byte_str=bytearray(
            bytes(header).ljust((free_space_end), b"\0") + schema_record_bytes  # noqa
        ),
        schema_record=schema_record,
        size=page_size,
    )
    record_to_add = pack("<3si", "abc".encode("utf-8"), 2)
    slotted_page.add_record(record_to_add)

    updated_slotted_page = SlottedPage.from_bytes(bytes(slotted_page.byte_str))

    assert (
        updated_slotted_page.header.free_space_start
        == SLOTTED_PAGE_HEADER_METADATA_SIZE + 2 * SLOT_SIZE  # noqa
    )
    assert updated_slotted_page.header.free_space_end == page_size - len(
        schema_record_bytes
    ) - len(record_to_add)
    assert updated_slotted_page.header.slots[1] == Slot(
        1, page_size - len(schema_record_bytes) - len(record_to_add), len(record_to_add)
    )
    assert updated_slotted_page.schema_record == schema_record
