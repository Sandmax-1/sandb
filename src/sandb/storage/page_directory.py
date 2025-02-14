from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from struct import pack, unpack_from
from typing import Iterable, Iterator

from sandb.storage.constants import INT_SIZE_IN_BYTES


@dataclass
class PagePointer(Iterable[int]):
    """
    Represents a pointer to a page with its ID and start position.

    Attributes:
        page_id (int): Identifier for the page.
        page_start (int): Byte offset where the page starts.
    """

    page_id: int
    page_start: int

    def __iter__(self) -> Iterator[int]:
        """
        Allows the PagePointer to be iterated over as a tuple (page_id, page_start).
        This facilitates encoding to bytes in PageDirectoryHeader.to_bytes.

        Yields:
            int: page_id, then page_start.
        """
        yield from (self.page_id, self.page_start)


@dataclass
class PageDirectoryHeader:
    """
    Represents the header for a page directory, containing metadata and page pointers.

    Attributes:
        page_directory_id (int): Identifier for the page directory.
        free_space_start (int): Byte offset where free space starts.
        directory_size (int): Size of the directory in bytes.
        page_pointers (list[PagePointer]): List of pointers to pages.
    """

    page_directory_id: int
    free_space_start: int
    directory_size: int
    page_pointers: list[PagePointer]

    def __bytes__(self) -> bytes:
        """
        Serializes the PageDirectoryHeader into a byte string.

        Returns:
            bytes: The byte representation of the page directory header.
        """
        byte_str = pack(
            f"<iiii{len(self.page_pointers) * 2}i",
            self.page_directory_id,
            self.free_space_start,
            self.directory_size,
            len(self.page_pointers),
            *chain(*self.page_pointers),
        )

        return byte_str.ljust(self.directory_size, b"\0")

    @classmethod
    def from_bytes(cls, byte_str: bytes) -> "PageDirectoryHeader":
        """
        Deserializes a byte string into a PageDirectoryHeader instance.

        Args:
            byte_str (bytes): Byte string representing the page directory header.

        Returns:
            PageDirectoryHeader: A new PageDirectoryHeader instance.
        """
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
        """
        Reads a PageDirectoryHeader from a file.

        Args:
            file_path (Path): Path to the file containing the page directory header.
            page_directory_offset (int): Offset where the page directory starts.
            page_directory_size_bytes (int): Size of the page directory in bytes.

        Returns:
            PageDirectoryHeader: A new PageDirectoryHeader instance populated from
                                 the file.
        """
        with open(file_path, "br") as f:
            f.seek(page_directory_offset)
            return PageDirectoryHeader.from_bytes(f.read(page_directory_size_bytes))
