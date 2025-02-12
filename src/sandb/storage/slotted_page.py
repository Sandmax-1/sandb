from dataclasses import dataclass
from itertools import chain
from struct import pack, unpack_from
from typing import Iterable, Iterator

from sandb.storage.constants import INT_SIZE_IN_BYTES
from sandb.storage.record import SchemaRecord


@dataclass
class Slot(Iterable[int]):
    """
    Represents a slot within a slotted page.
    Each slot contains a record ID, a pointer to the record's location,
    and the record's length.
    """

    record_id: int
    record_pointer: int
    record_length: int

    def to_bytes(self) -> bytes:
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
    Represents the header of a slotted page, including metadata and slot directory.
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

    def to_bytes(self) -> bytes:
        """
        Serializes the SlottedPageHeader to a byte string.

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

        offset += INT_SIZE_IN_BYTES * 4 - 1

        slots_raw = unpack_from("<" + f"{len_slots * 3}i", byte_str, offset)
        offset += 3 * INT_SIZE_IN_BYTES * len_slots
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
    header: SlottedPageHeader
    byte_str: bytearray
    schema_record: SchemaRecord
    size: int = 150

    @classmethod
    def from_bytes(cls, byte_str: bytes) -> "SlottedPage":
        slotted_page_header = SlottedPageHeader.from_bytes(byte_str)
        schema_offset = slotted_page_header.slots[0].record_pointer

        schema_record = SchemaRecord.from_bytes(byte_str[schema_offset:])

        return SlottedPage(slotted_page_header, bytearray(byte_str), schema_record)

    def add_record(self, record: bytes) -> None:
        # TODO: How do I make all this atomic?
        record_length = len(record)

        slot = Slot(
            record_id=self.header.next_row_id,
            record_pointer=self.header.free_space_end + 1 - record_length,
            record_length=record_length,
        )
        self.header.next_row_id += 1

        self.header.slots.append(slot)

        self.byte_str[
            self.header.free_space_start : self.header.free_space_start
            + 3 * INT_SIZE_IN_BYTES  # noqa
        ] = slot.to_bytes()

        self.byte_str[
            self.header.free_space_end - record_length : self.header.free_space_end
        ] = record

        self.byte_str[4:17] = bytearray(
            pack(
                "<iii",
                self.header.free_space_start + 3 * INT_SIZE_IN_BYTES,
                self.header.free_space_end - record_length,
                len(self.header.slots),
            )
        )
        self.is_dirty = True
