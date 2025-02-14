from dataclasses import dataclass
from itertools import chain
from struct import pack, unpack_from
from typing import Iterable, Iterator

from sandb.storage.constants import INT_SIZE_IN_BYTES
from sandb.storage.record import SchemaRecord

SLOT_SIZE = 3 * INT_SIZE_IN_BYTES
SLOTTED_PAGE_HEADER_METADATA_SIZE = 4 * INT_SIZE_IN_BYTES


class PageFullException(Exception):
    """
    Exception to raise if we don't have enough space
    to add a record to the slotted page.
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
    free_space_start: int
    free_space_end: int
    slots: list[Slot]
    is_dirty: bool = False
    next_row_id: int = 0
    # checksum: int | None  # TODO implement this later
    # TODO: Should I include all my records here once loaded into memory?
    #       Or retrieve from disk using the pointers?

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


@dataclass
class SlottedPage:
    """
    Represents a slotted page, which is a unit of storage on
    disk.

    A slotted page contains a header (SlottedPageHeader),
    a byte array to store page content, and schema information
    (SchemaRecord). It manages records using a slot directory
    within the header.
    """

    header: SlottedPageHeader
    byte_str: bytearray
    schema_record: SchemaRecord
    size: int = 4096

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
        schema_offset = slotted_page_header.slots[0].record_pointer

        schema_record = SchemaRecord.from_bytes(byte_str[schema_offset:])

        return SlottedPage(slotted_page_header, bytearray(byte_str), schema_record)

    def add_record(self, record: bytes) -> None:
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

        slot = Slot(
            record_id=self.header.next_row_id,
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

        # Update header metadata: move free space pointers, serialize and write header
        self.header.free_space_start += SLOT_SIZE  # Slot directory grows forward
        self.header.free_space_end -= record_length  # Record area grows backward
        serialised_header = bytes(self.header)
        self.byte_str[: len(serialised_header)] = serialised_header

        self.is_dirty = True
