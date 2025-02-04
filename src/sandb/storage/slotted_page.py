from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from struct import pack, unpack
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

    def to_binary(self, file_path: Path, page_start_offset: int) -> None:
        with open(file_path, "ab") as f:
            f.seek(page_start_offset)
            f.write(
                pack(
                    "<iiii",
                    self.page_id,
                    self.free_space_start,
                    self.free_space_end,
                    len(self.slots),
                )
            )

            f.write(
                pack(
                    "<" + f"{len(self.slots) * 3}i",
                    *chain(*self.slots),
                )
            )

    @classmethod
    def from_binary(
        cls, file_path: Path, page_start_offset: int
    ) -> "SlottedPageHeader":
        with open(file_path, "rb") as f:
            f.seek(page_start_offset)
            (
                page_id,
                free_space_start,
                free_space_end,
                len_slots,
            ) = unpack("<iiii", f.read(4 * INT_SIZE_IN_BYTES))

            slots_raw = unpack(
                "<" + f"{len_slots * 3}i",
                f.read(3 * INT_SIZE_IN_BYTES * len_slots),
            )
            slots = [
                Slot(
                    record_id=slots_raw[slot_ind],
                    record_pointer=slots_raw[slot_ind + 1],
                    record_length=slots_raw[slot_ind + 2],
                )
                for slot_ind in range(0, 3 * len_slots, 3)
            ]

            records_data = f.read(sum(slot.record_length for slot in slots))
            offset = 0
            records: list[bytes] = []

            for slot in slots:
                records.append(records_data[offset : offset + slot.record_length])
                offset += slot.record_length

            return SlottedPageHeader(
                page_id, free_space_start, free_space_end, slots, records
            )
