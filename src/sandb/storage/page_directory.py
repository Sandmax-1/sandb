from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from struct import pack, unpack_from
from typing import Iterable, Iterator

from sandb.storage.constants import INT_SIZE_IN_BYTES


@dataclass
class PagePointer(Iterable[int]):
    page_id: int
    page_start: int

    def __iter__(self) -> Iterator[int]:
        """
        Doing this such that we can encode to bytes easier in
        PageDirectoryHeader.to_binary. Shouldn't really be used otherwise.

        Yields:
            Iterator[int]: page_id, then page_start
        """
        yield from (self.page_id, self.page_start)


@dataclass
class PageDirectoryHeader:
    page_directory_id: int
    free_space_start: int
    directory_size: int
    page_pointers: list[PagePointer]

    def to_bytes(self) -> bytes:
        return pack(
            f"<iiii{len(self.page_pointers) * 2}i",
            self.page_directory_id,
            self.free_space_start,
            self.directory_size,
            len(self.page_pointers),
            *chain(*self.page_pointers),
        )

    @classmethod
    def from_bytes(cls, byte_str: bytes) -> "PageDirectoryHeader":
        offset = 0
        (
            page_directory_id,
            free_space_start,
            directory_size,
            len_page_pointers,
        ) = unpack_from("<iiii", byte_str, offset)

        offset += INT_SIZE_IN_BYTES * 4

        page_pointers_raw = unpack_from(
            "<" + f"{len_page_pointers * 2}i", byte_str, offset
        )
        page_pointers = [
            PagePointer(
                page_pointers_raw[page_pointers_ind],
                page_pointers_raw[page_pointers_ind + 1],
            )
            for page_pointers_ind in range(0, 2 * len_page_pointers, 2)
        ]

        return PageDirectoryHeader(
            page_directory_id, free_space_start, directory_size, page_pointers
        )

    @classmethod
    def from_file(
        cls, file_path: Path, page_directory_offset: int, page_directory_size_bytes: int
    ) -> "PageDirectoryHeader":
        with open(file_path, "br") as f:
            f.seek(page_directory_offset)
            return PageDirectoryHeader.from_bytes(f.read(page_directory_size_bytes))
