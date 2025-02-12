from struct import pack

from sandb.storage.record import SchemaRecord
from sandb.storage.slotted_page import Slot, SlottedPage, SlottedPageHeader


def test_slotted_page_header_serialisation() -> None:
    slotted_page_header = SlottedPageHeader(
        page_id=0,
        free_space_start=28,
        free_space_end=32,
        slots=[Slot(record_id=0, record_pointer=33, record_length=4)],
    )

    assert slotted_page_header == SlottedPageHeader.from_bytes(
        slotted_page_header.to_bytes()
    )


def test_slotted_page_add_record() -> None:
    header = SlottedPageHeader(
        page_id=0,
        free_space_start=28,
        free_space_end=101,
        slots=[Slot(record_id=0, record_pointer=101, record_length=4)],
        next_row_id=1,
    )

    schema_record = SchemaRecord(
        0, "table_1", dtypes=[1, 0], col_names=["str_col", "int_col"]
    )
    schema_record_bytes = schema_record.to_bytes()

    slotted_page = SlottedPage(
        header=header,
        byte_str=bytearray(
            header.to_bytes().ljust((150 - len(schema_record_bytes)), b"\0")
            + schema_record_bytes  # noqa
        ),
        schema_record=schema_record,
        size=150,
    )
    slotted_page.add_record(pack("<3si", "abc".encode("utf-8"), 2))

    updated_slotted_page = SlottedPage.from_bytes(bytes(slotted_page.byte_str))

    assert updated_slotted_page.header.free_space_start == 40
    assert updated_slotted_page.header.free_space_end == 94
    assert updated_slotted_page.header.slots[1] == Slot(1, 94, 7)
    assert updated_slotted_page.schema_record == schema_record
