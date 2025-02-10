from struct import pack

from sandb.storage.slotted_page import Slot, SlottedPageHeader


def test_slotted_page_serialisation() -> None:
    slotted_page_header = SlottedPageHeader(
        page_id=0,
        free_space_start=28,
        free_space_end=32,
        slots=[Slot(record_id=0, record_pointer=33, record_length=4)],
        records=[pack("<i", 84)],
    )

    assert slotted_page_header == SlottedPageHeader.from_bytes(
        slotted_page_header.to_bytes()
    )
