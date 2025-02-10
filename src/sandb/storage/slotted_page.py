from dataclasses import dataclass
from itertools import chain
from struct import pack, unpack_from
from typing import Iterable, Iterator

from sandb.storage.constants import INT_SIZE_IN_BYTES


@dataclass
class Slot(Iterable[int]):
    record_id: int
    record_pointer: int
    record_length: int

    def __iter__(self) -> Iterator[int]:
        """
        Doing this such that we can encode to bytes easier in
        PageDirectoryHeader.to_binary. Shouldn't really be used otherwise.

        Yields:
            Iterator[int]: record_id, then record_pointer, then record_length
        """
        yield from (self.record_id, self.record_pointer, self.record_length)


@dataclass
class SlottedPageHeader:
    page_id: int
    free_space_start: int
    free_space_end: int
    slots: list[Slot]
    records: list[bytes]
    # checksum: int | None  # TODO implement this later
    # TODO: Should I include all my records here once loaded into memory?
    #       Or retrieve from disk using the pointers?

    def to_bytes(self) -> bytes:
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

        for record in self.records:
            byte_str += record

        return byte_str

    @classmethod
    def from_bytes(cls, byte_str: bytes) -> "SlottedPageHeader":
        offset = 0
        (
            page_id,
            free_space_start,
            free_space_end,
            len_slots,
        ) = unpack_from("<iiii", byte_str, offset)

        offset += INT_SIZE_IN_BYTES * 4

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
        records: list[bytes] = []
        offset = free_space_end

        for slot in slots:
            records.append(byte_str[offset : offset + slot.record_length])
            offset += slot.record_length

        return SlottedPageHeader(
            page_id, free_space_start, free_space_end, slots, records
        )
