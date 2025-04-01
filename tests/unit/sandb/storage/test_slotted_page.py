import pytest

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


def test_add_record_successful(slotted_page_empty: SlottedPage) -> None:
    """Test successful addition of a record to the slotted page."""
    slotted_page = slotted_page_empty
    record_data = b"test_record_data"
    record_id = slotted_page.add_record(record_data)

    assert record_id == 0
    assert len(slotted_page.header.slots) == 1
    slot = slotted_page.header.slots[0]
    assert slot.record_id == 0
    assert slot.record_length == len(record_data)
    assert slot.record_pointer == slotted_page.size - len(record_data)
    assert (
        slotted_page.header.free_space_start
        == SLOTTED_PAGE_HEADER_METADATA_SIZE + SLOT_SIZE
    )
    assert slotted_page.header.free_space_end == slotted_page.size - len(record_data)
    assert slotted_page.is_dirty is True
    assert (
        slotted_page.byte_str[
            slot.record_pointer : slot.record_pointer + slot.record_length
        ]
        == record_data
    )


def test_add_record_page_full(slotted_page_empty: SlottedPage) -> None:
    """Test adding a record that exceeds the page's free space,
    raising PageFullException."""
    slotted_page = slotted_page_empty
    page_size = slotted_page.size
    header_size_with_initial_slot = SLOTTED_PAGE_HEADER_METADATA_SIZE + SLOT_SIZE

    max_record_size = page_size - header_size_with_initial_slot
    record_data = b"A" * max_record_size

    slotted_page.add_record(record_data)

    record_data_too_large = b"TOO_LARGE"
    with pytest.raises(PageFullException):
        slotted_page.add_record(record_data_too_large)


def test_add_record_multiple_records(slotted_page_empty: SlottedPage) -> None:
    """Test adding multiple records to the slotted page."""
    slotted_page = slotted_page_empty
    record_data1 = b"record_1"
    record_data2 = b"record_longer_2"
    record_data3 = b"record_3"

    record_id1 = slotted_page.add_record(record_data1)
    record_id2 = slotted_page.add_record(record_data2)
    record_id3 = slotted_page.add_record(record_data3)

    assert record_id1 == 0
    assert record_id2 == 1
    assert record_id3 == 2
    assert len(slotted_page.header.slots) == 3

    slot3 = slotted_page.header.slots[2]
    assert slot3.record_id == 2
    assert slot3.record_length == len(record_data3)
    assert slot3.record_pointer == slotted_page.size - len(record_data1) - len(
        record_data2
    ) - len(record_data3)

    # Check free space pointers
    expected_free_space_start = SLOTTED_PAGE_HEADER_METADATA_SIZE + 3 * SLOT_SIZE
    expected_free_space_end = (
        slotted_page.size - len(record_data1) - len(record_data2) - len(record_data3)
    )
    assert slotted_page.header.free_space_start == expected_free_space_start
    assert slotted_page.header.free_space_end == expected_free_space_end
    assert slotted_page.is_dirty is True
    assert (
        slotted_page.byte_str[
            slot3.record_pointer : slot3.record_pointer + slot3.record_length
        ]
        == record_data3
    )
    assert slotted_page.header.next_row_id == 3


def test_add_record_zero_length_record(slotted_page_empty: SlottedPage) -> None:
    """Test adding a zero-length record."""
    slotted_page = slotted_page_empty
    record_data = b""
    record_id = slotted_page.add_record(record_data)

    assert record_id == 0
    assert len(slotted_page.header.slots) == 1
    slot = slotted_page.header.slots[0]
    assert slot.record_id == 0
    assert slot.record_length == 0
    assert slot.record_pointer == slotted_page.size
    assert (
        slotted_page.header.free_space_start
        == SLOTTED_PAGE_HEADER_METADATA_SIZE + SLOT_SIZE
    )
    assert slotted_page.header.free_space_end == slotted_page.size
    assert slotted_page.is_dirty is True


def test_add_record_updates_header_bytes(slotted_page_empty: SlottedPage) -> None:
    """Test that adding a record correctly updates the header bytes in byte_str."""
    slotted_page = slotted_page_empty
    initial_header_bytes = bytes(slotted_page.header)

    record_data = b"test_record"
    slotted_page.add_record(record_data)
    updated_header_bytes = bytes(slotted_page.header)

    assert updated_header_bytes != initial_header_bytes
    deserialized_header = SlottedPageHeader.from_bytes(bytes(slotted_page.byte_str))
    assert deserialized_header == slotted_page.header


def test_add_record_exactly_fills_remaining_space(
    slotted_page_empty: SlottedPage,
) -> None:
    """Test adding a record that exactly fills the remaining space on the page."""
    slotted_page = slotted_page_empty
    initial_free_space = (
        slotted_page.header.free_space_end - slotted_page.header.free_space_start
    )
    record_size = initial_free_space - SLOT_SIZE

    record_data = b"B" * record_size
    record_id = slotted_page.add_record(record_data)

    assert record_id == 0
    assert len(slotted_page.header.slots) == 1
    slot = slotted_page.header.slots[0]
    assert slot.record_length == record_size
    assert (
        slotted_page.header.free_space_end
        == SLOTTED_PAGE_HEADER_METADATA_SIZE + SLOT_SIZE
    )
    assert slotted_page.header.free_space_start == slotted_page.header.free_space_end


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

    assert slotted_page.is_dirty


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

    assert slotted_page.is_dirty
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

    assert slotted_page.is_dirty
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

    assert slotted_page.is_dirty
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


def test_get_record_successful(slotted_page_empty: SlottedPage) -> None:
    page = slotted_page_empty
    record_data = "test record data".encode("utf-8")
    record_id = page.add_record(record_data)

    retrieved_record = page.get_record(record_id)

    assert retrieved_record == record_data


def test_get_record_non_existent_record_id(slotted_page_empty: SlottedPage) -> None:
    page = slotted_page_empty
    non_existent_record_id = 999

    with pytest.raises(RecordNotInPage) as exc_info:
        page.get_record(non_existent_record_id)
    assert (
        str(exc_info.value)
        == f"Record ID {non_existent_record_id} not found in page {page.page_id}."
    )


def test_get_record_deleted_record(slotted_page_empty: SlottedPage) -> None:
    page = slotted_page_empty
    record_data = b"record to be deleted"
    record_id = page.add_record(record_data)
    page.delete(record_id)

    with pytest.raises(RecordNotInPage) as exc_info:
        page.get_record(record_id)
    assert (
        str(exc_info.value)
        == f"""Record ID {record_id} exists in page {page.page_id} but
is marked as deleted."""
    )


def test_get_record_multiple_records(slotted_page_empty: SlottedPage) -> None:
    """Test get_record works correctly with multiple records in the page."""
    page = slotted_page_empty
    record_data_1 = "first record".encode("utf-8")
    record_data_2 = "second record".encode("utf-8")
    record_id_1 = page.add_record(record_data_1)
    record_id_2 = page.add_record(record_data_2)

    retrieved_record_1 = page.get_record(record_id_1)
    retrieved_record_2 = page.get_record(record_id_2)

    assert retrieved_record_1 == record_data_1
    assert retrieved_record_2 == record_data_2


def test_get_record_after_update_smaller_record(
    slotted_page_empty: SlottedPage,
) -> None:
    page = slotted_page_empty
    initial_record_data = "initial record data".encode("utf-8")
    updated_record_data = "updated".encode("utf-8")
    record_id = page.add_record(initial_record_data)
    page.update(record_id, updated_record_data)

    retrieved_record = page.get_record(record_id)
    assert retrieved_record == updated_record_data


def test_get_record_after_update_larger_record(slotted_page_empty: SlottedPage) -> None:
    page = slotted_page_empty
    initial_record_data = "small record".encode("utf-8")
    updated_record_data = (
        "a much larger record for update that causes relocation".encode("utf-8")
    )
    record_id = page.add_record(initial_record_data)
    updated_record_id = page.update(record_id, updated_record_data)

    retrieved_record = page.get_record(updated_record_id)
    assert retrieved_record == updated_record_data

    with pytest.raises(RecordNotInPage):
        page.get_record(record_id)
