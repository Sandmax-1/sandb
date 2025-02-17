from struct import pack

import pytest

from sandb.storage.record import SchemaRecord
from sandb.storage.slotted_page import (
    SLOT_SIZE,
    SLOTTED_PAGE_HEADER_METADATA_SIZE,
    PageFullException,
    RecordNotInPage,
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


def test_slotted_page_add_record(schema_record: SchemaRecord) -> None:
    page_size = 150
    schema_record_bytes = bytes(schema_record)

    header = SlottedPageHeader(
        page_id=0,
        free_space_start=SLOTTED_PAGE_HEADER_METADATA_SIZE,
        free_space_end=page_size,
        slots=[],
        next_row_id=0,
    )

    slotted_page = SlottedPage(
        header=header,
        byte_str=bytearray(bytes(header).ljust((page_size), b"\0")),
        size=page_size,
    )
    slotted_page.add_record(bytes(schema_record))
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


def test_delete_record_successful(slotted_page: SlottedPage) -> None:
    slotted_page = slotted_page
    original_slot = slotted_page.header.slots[1]
    record_id_to_delete = original_slot.record_id
    slotted_page.delete(record_id_to_delete)

    deleted_slot = next(
        (
            slot
            for slot in slotted_page.header.slots
            if slot.record_id == record_id_to_delete
        ),
        None,
    )
    assert deleted_slot is not None
    assert deleted_slot.record_pointer == 0

    assert slotted_page.header.is_dirty


def test_delete_record_not_found(slotted_page: SlottedPage) -> None:
    record_id_to_delete = 999
    with pytest.raises(RecordNotInPage):
        slotted_page.delete(record_id_to_delete)


def test_update_record_in_place_smaller(slotted_page: SlottedPage) -> None:
    original_slot = slotted_page.header.slots[2]

    new_record_data = b"record_new"
    updated_record_id = slotted_page.update(original_slot.record_id, new_record_data)

    updated_slot = next(
        (
            slot
            for slot in slotted_page.header.slots
            if slot.record_id == original_slot.record_id
        )
    )
    assert updated_slot.record_pointer == original_slot.record_pointer
    assert updated_slot.record_length == len(new_record_data)
    assert (
        slotted_page.byte_str[
            updated_slot.record_pointer : updated_slot.record_pointer
            + updated_slot.record_length
        ]
        == new_record_data
    )

    assert slotted_page.header.is_dirty
    assert updated_record_id == original_slot.record_id


def test_update_record_in_place_same_size(slotted_page: SlottedPage) -> None:
    original_slot = slotted_page.header.slots[3]
    record_id_to_update = original_slot.record_id
    original_pointer = original_slot.record_pointer

    new_record_data = b"record_data_3"
    updated_record_id = slotted_page.update(record_id_to_update, new_record_data)
    updated_slot = next(
        (
            slot
            for slot in slotted_page.header.slots
            if slot.record_id == record_id_to_update
        )
    )
    assert updated_slot.record_pointer == original_pointer
    assert updated_slot.record_length == len(new_record_data)
    assert (
        slotted_page.byte_str[
            updated_slot.record_pointer : updated_slot.record_pointer
            + updated_slot.record_length
        ]
        == new_record_data
    )

    assert slotted_page.header.is_dirty
    assert updated_record_id == record_id_to_update


def test_update_record_relocation_larger_fits(slotted_page: SlottedPage) -> None:
    original_slot = slotted_page.header.slots[1]
    original_pointer = original_slot.record_pointer
    record_id_to_update = original_slot.record_id

    new_record_data = b"record_data_1_much_longer"
    updated_record_id = slotted_page.update(record_id_to_update, new_record_data)

    updated_slot = next(
        (
            slot
            for slot in slotted_page.header.slots
            if slot.record_id == updated_record_id
        )
    )
    assert updated_slot.record_pointer != original_pointer
    assert updated_slot.record_length == len(new_record_data)
    assert (
        slotted_page.byte_str[
            updated_slot.record_pointer : updated_slot.record_pointer
            + updated_slot.record_length
        ]
        == new_record_data
    )

    original_slot_deleted = next(
        (
            slot
            for slot in slotted_page.header.slots
            if slot.record_id == record_id_to_update
        ),
        None,
    )
    assert original_slot_deleted is not None
    assert original_slot_deleted.record_pointer == 0

    assert slotted_page.header.is_dirty
    assert updated_record_id != record_id_to_update


def test_update_record_relocation_larger_page_full() -> None:
    page_almost_full_header = SlottedPageHeader(
        page_id=2, free_space_start=4050, free_space_end=4096, slots=[]
    )
    page_almost_full = SlottedPage(
        header=page_almost_full_header,
        byte_str=bytearray(4096),
    )
    record_id_page_full = page_almost_full.add_record(b"initial_record")

    record_id_to_update = record_id_page_full
    new_record_data = b"much_larger_record_that_wont_fit"

    with pytest.raises(PageFullException):
        page_almost_full.update(record_id_to_update, new_record_data)


def test_update_record_not_found(slotted_page: SlottedPage) -> None:
    record_id_to_update = 999
    new_record_data = b"new_data"
    with pytest.raises(RecordNotInPage):
        slotted_page.update(record_id_to_update, new_record_data)
