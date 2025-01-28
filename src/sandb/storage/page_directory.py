from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from struct import pack, unpack


@dataclass
class PagePointer:
    page_id: int
    page_start: int

    def __iter__(self):
        yield from (self.page_id, self.page_start)


@dataclass
class PageDirectoryHeader:
    page_directory_id: int
    free_space_start: int
    directory_size: int
    page_pointers: list[PagePointer]

    def to_binary(self, file_path: Path, directory_start_offset: int):
        with open(file_path, "wb") as f:
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
            print(
                list(chain(*self.page_pointers)), "<" + "ii" * len(self.page_pointers)
            )
            f.write(
                pack("<" + "ii" * len(self.page_pointers), *chain(*self.page_pointers))
            )

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
            ) = unpack("<iiii", f.read(16))

            page_pointers = unpack(
                "<" + "ii" * len_page_pointers, f.read(8 * len_page_pointers)
            )
            page_pointers = [
                PagePointer(
                    page_pointers[page_pointers_ind],
                    page_pointers[page_pointers_ind + 1],
                )
                for page_pointers_ind in range(0, 2 * len_page_pointers, 2)
            ]

            return PageDirectoryHeader(
                page_directory_id, free_space_start, directory_size, page_pointers
            )
