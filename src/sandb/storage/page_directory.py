import os
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from struct import pack, unpack
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

    def to_binary(self, file_path: Path, directory_start_offset: int) -> None:
        # TODO: For some reason I have to use 'ab' here not 'wb'
        #       I think it's something to do with bnary file endings,
        #       but I would need to investigate further.
        #       I think this is fine for now as we're only going to be using
        #       to_binary when there is nothing else at the end of the file
        #       so we can append no problem.
        with open(file_path, "ab") as f:
            f.seek(directory_start_offset)
            f.write(
                pack(
                    "<iiii",
                    self.page_directory_id,
                    self.free_space_start,
                    self.directory_size,
                    len(self.page_pointers),
                )
            )

            f.write(
                pack(
                    "<" + f"{len(self.page_pointers) * 2}i",
                    *chain.from_iterable(self.page_pointers),
                )
            )
        print(f"size after page directory: {os.path.getsize(file_path)}")

    @classmethod
    def from_binary(
        cls, file_path: Path, directory_start_offset: int
    ) -> "PageDirectoryHeader":
        with open(file_path, "rb") as f:
            f.seek(directory_start_offset)
            (
                page_directory_id,
                free_space_start,
                directory_size,
                len_page_pointers,
            ) = unpack("<iiii", f.read(4 * INT_SIZE_IN_BYTES))

            page_pointers_raw = unpack(
                "<" + f"{len_page_pointers * 2}i",
                f.read(2 * INT_SIZE_IN_BYTES * len_page_pointers),
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
