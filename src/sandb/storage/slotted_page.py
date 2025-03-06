from dataclasses import dataclass, field
from itertools import chain
from struct import pack, unpack_from
from typing import Iterable, Iterator

from sandb.storage.constants import INT_SIZE_IN_BYTES

SLOT_SIZE = 3 * INT_SIZE_IN_BYTES
SLOTTED_PAGE_HEADER_METADATA_SIZE = 4 * INT_SIZE_IN_BYTES


class PageFullException(Exception):
    """
    Exception to raise if we don't have enough space
    to add a record to the slotted page.
    """

    ...


class RecordNotInPage(Exception):
    """
    Exception to raise when a record id is not found in a given page
    """

    ...


@dataclass
class Slot(Iterable[int]):
    """
    Represents a slot within a slotted page.

    Each slot contains metadata about a record: record ID,
    record pointer (offset), and record length.
    Slots are stored in the SlottedPageHeader's slot directory.
    """

    record_id: int
    record_pointer: int
    record_length: int

    def __bytes__(self) -> bytes:
        """
        Serializes the Slot object into a byte string.

        Returns:
            bytes: Byte representation of the slot
                   (record_id, record_pointer, record_length).
        """
        return pack("<iii", self.record_id, self.record_pointer, self.record_length)

    def __iter__(self) -> Iterator[int]:
        """
        Enables iteration over slot attributes to facilitate serialization.

        Yields:
            Iterator[int]: record_id, record_pointer, record_length
        """
        yield from (self.record_id, self.record_pointer, self.record_length)


@dataclass
class SlottedPageHeader:
    """
    Represents the header of a slotted page.

    The header contains metadata about the page and the slot directory,
    which points to records stored within the page's byte array.
    """

    page_id: int
    free_space_start: int = 0
    free_space_end: int = 0
    slots: list[Slot] = field(default_factory=list)
    next_row_id: int = 0
    # checksum: int | None  # TODO implement this later

    def __post_init__(self) -> None:
        if self.slots:
            self.next_row_id = max([slot.record_id for slot in self.slots]) + 1

    def __bytes__(self) -> bytes:
        """
        Serializes the SlottedPageHeader to a byte string.

        The serialized format includes:
        - page_id (int)
        - free_space_start (int)
        - free_space_end (int)
        - number of slots (int)
        - slot directory (array of Slot objects, each serialized to bytes)

        Returns:
            bytes: Serialized representation of the header and its slots.
        """
        byte_str = pack(
            "<iiii",
            self.page_id,
            self.free_space_start,
            self.free_space_end,
            len(self.slots),
        )

        byte_str = byte_str + pack(
            "<" + f"{len(self.slots) * 3}i",
            *chain(*self.slots),
        )

        byte_str = byte_str.ljust(self.free_space_end, b"\0")

        return byte_str

    @classmethod
    def from_bytes(cls, byte_str: bytes) -> "SlottedPageHeader":
        """
        Deserializes a byte string into a SlottedPageHeader instance.

        Args:
            byte_str (bytes): Serialized byte representation of a SlottedPageHeader.

        Returns:
            SlottedPageHeader: The deserialized header object.
        """
        offset = 0
        (
            page_id,
            free_space_start,
            free_space_end,
            len_slots,
        ) = unpack_from("<iiii", byte_str, offset)

        offset += SLOTTED_PAGE_HEADER_METADATA_SIZE

        slots_raw = unpack_from("<" + f"{len_slots * 3}i", byte_str, offset)
        offset += SLOT_SIZE * len_slots
        slots = [
            Slot(
                record_id=slots_raw[slot_ind],
                record_pointer=slots_raw[slot_ind + 1],
                record_length=slots_raw[slot_ind + 2],
            )
            for slot_ind in range(0, 3 * len_slots, 3)
        ]

        return SlottedPageHeader(
            page_id,
            free_space_start,
            free_space_end,
            slots,
        )


class SlottedPage:
    """
    Represents a slotted page, which is a unit of storage on
    disk.

    A slotted page contains a header (SlottedPageHeader),
    a byte array to store page content, and schema information
    (SchemaRecord). It manages records using a slot directory
    within the header.
    """

    def __init__(
        self,
        page_id: int,
        header: SlottedPageHeader | None = None,
        byte_str: bytearray | None = None,
        size: int = 4096,
    ) -> None:
        self.page_id = page_id
        self.size = size
        self.is_dirty = False

        if header:
            self.header = header

        else:
            self.header = SlottedPageHeader(
                page_id=self.page_id,
                free_space_start=SLOTTED_PAGE_HEADER_METADATA_SIZE,
                free_space_end=self.size,
            )

        if byte_str:
            self.byte_str = byte_str

        else:
            self.byte_str = bytearray(bytes(self.header).ljust(self.size, b"\0"))

    def _update_byte_str_with_header(self) -> None:
        serialised_header = bytes(self.header)
        self.byte_str[: len(serialised_header)] = serialised_header

    def __bytes__(self) -> bytes:
        return bytes(self.byte_str)

    @classmethod
    def from_bytes(cls, byte_str: bytes) -> "SlottedPage":
        """
        Creates a SlottedPage object by deserializing from a byte string.

        This method reconstructs the SlottedPageHeader and SchemaRecord
        from the byte string.

        Args:
            byte_str (bytes): Byte representation of the entire slotted
            page (header + data).

        Returns:
            SlottedPage: A SlottedPage object deserialized from the byte string.
        """
        slotted_page_header = SlottedPageHeader.from_bytes(byte_str)

        return SlottedPage(
            slotted_page_header.page_id, slotted_page_header, bytearray(byte_str)
        )

    def add_record(self, record: bytes) -> int:
        """
        Adds a new record to the slotted page.

        This method adds the record data to the page's byte array and creates a new slot
        in the SlottedPageHeader's slot directory to point to the record.

        Args:
            record (bytes): The byte data of the record to be added.

        Raises:
            PageFullException: If there is not enough free space on the page to add the
            record.
        """
        record_length = len(record)
        required_space = record_length + SLOT_SIZE
        available_space = self.header.free_space_end - self.header.free_space_start

        if required_space > available_space:
            raise PageFullException(
                f"""Can't add record to page {self.header.page_id} as not enough space.
                    We have {available_space} bytes free and the record is
                    {required_space} in length."""
            )

        record_id = self.header.next_row_id

        slot = Slot(
            record_id=record_id,
            record_pointer=self.header.free_space_end - record_length,
            record_length=record_length,
        )
        self.header.next_row_id += 1

        self.header.slots.append(slot)

        # Write record data to the free space area
        # at the end of the page growing backwards
        record_start_offset = self.header.free_space_end - record_length
        record_end_offset = self.header.free_space_end
        self.byte_str[record_start_offset:record_end_offset] = record

        self.header.free_space_start += SLOT_SIZE
        self.header.free_space_end -= record_length

        self._update_byte_str_with_header()

        self.is_dirty = True
        return record_id

    def delete(self, record_id: int) -> None:
        """
        Deletes a record from the slotted page by record ID.

        This method logically deletes a record by invalidating its slot
        (setting record_pointer to 0). The record data remains in the
        byte array, but the space is not immediately reclaimed.

        Args:
            record_id (int): The ID of the record to delete.

        Raises:
            RecordNotInPage: If the record_id is not found in the page.

        Side Effects:
            - Modifies the slot directory in the SlottedPageHeader.
            - Updates the SlottedPageHeader in the byte_str.
            - Sets the is_dirty flag on the SlottedPageHeader.
        """

        have_modified = False
        for slot in self.header.slots:
            if slot.record_id == record_id:
                slot.record_pointer = 0
                have_modified = True
                break

        if have_modified:
            self._update_byte_str_with_header()
            self.is_dirty = True

        else:
            raise RecordNotInPage(
                f"Could not find record: {record_id} in page: {self.header.page_id}"
            )

    def update(self, record_id: int, record: bytes) -> int:
        """
        Updates an existing record in the slotted page.

        If the new record is smaller than or equal to the old record's size,
        it performs an in-place update. If the new record is larger, it
        attempts to relocate the record: adds the new record to available
        space and marks the old record's slot as deleted.

        Args:
            record_id (int): The ID of the record to update.
            record (bytes): The new byte data for the record.

        Returns:
            int: The record_id of the updated record. This will be the original
                 record_id for in-place updates, or a new record_id if relocation
                 was necessary.

        Raises:
            RecordNotInPage: If the record_id is not found in the page.
            PageFullException: If relocation is needed but there is not enough
                               space on the page to add the new, larger record.

        Side Effects:
            - Modifies the record data in byte_str (in-place or by relocation).
            - Updates the slot directory in SlottedPageHeader (slot length or marks
              slot as deleted during relocation).
            - Updates free space pointers in SlottedPageHeader (during relocation).
            - Updates the SlottedPageHeader in the byte_str.
            - Sets the is_dirty flag on the SlottedPageHeader.
        """
        record_in_page = False
        new_record_length = len(record)
        for slot in self.header.slots:
            if slot.record_id == record_id:
                record_in_page = True
                if new_record_length <= slot.record_length:
                    self.byte_str[
                        slot.record_pointer : slot.record_pointer + new_record_length
                    ] = record
                    slot.record_length = new_record_length
                    self._update_byte_str_with_header()

                else:
                    try:
                        new_record_id = self.add_record(record)
                        self.delete(record_id)
                        record_id = new_record_id
                    except PageFullException:
                        raise PageFullException(
                            f"""Can't update record: {record_id} as
                                not enough space in page"""
                        )
        if not record_in_page:
            raise RecordNotInPage(
                f"Could not find record: {record_id} in page: {self.header.page_id}"
            )
        self.is_dirty = True
        return record_id
