from dataclasses import dataclass, field
from itertools import chain
from pathlib import Path
from struct import pack, unpack_from
from typing import Iterable, Iterator

from sandb.storage.constants import INT_SIZE_IN_BYTES
from sandb.storage.slotted_page import SlottedPage
from sandb.storage.table_schema_page import TableSchemaPage

PAGE_POINTER_SIZE = 2 * INT_SIZE_IN_BYTES


class PageNotInDirectory(Exception):
    ...


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
class PageDirectory:
    """
    Represents the header for a page directory, containing metadata and page pointers.

    The PageDirectory manages the mapping between page IDs and their locations
    within the database file. It also handles creating new pages, reading pages
    from disk, and writing pages to disk.

    Attributes:
        file_path (Path): Path to the database file.
        page_start_offset (int): Byte offset in the database file where RecordStorage
                                 begins. Page offsets in PagePointers are relative to
                                 this offset.
        page_directory_id (int): Identifier for this page directory.
        page_size (int): Size of each page in bytes, including header and data.
        page_pointers (list[PagePointer]): List of PagePointer objects, each pointing to
                                           a page.
        next_page_id (int): The next available page ID to be assigned to a newly
                            created page.
        free_space_start (int): Byte offset within the PageDirectory page where free
                                space begins. Used to track where new page pointers
                                can be added.
    """

    file_path: Path
    page_start_offset: int
    page_directory_id: int
    page_size: int
    page_pointers: list[PagePointer] = field(
        default_factory=list
    )  # TODO: Probably want this as a hash table?
    next_page_id: int = 0
    free_space_start: int = 0

    def __post_init__(self) -> None:
        if self.page_pointers:
            self.next_page_id = (
                max([page_pointer.page_id for page_pointer in self.page_pointers]) + 1
            )
        else:
            # First page always has to be the page directory.
            self.page_pointers.append(PagePointer(0, 0))
            self.next_page_id = 1
            with open(self.file_path, "r+b") as f:
                f.seek(self.page_start_offset)
                f.write(bytes(self))

        self.free_space_start = (
            2 * INT_SIZE_IN_BYTES + len(self.page_pointers) * PAGE_POINTER_SIZE
        )

    def __bytes__(self) -> bytes:
        """
        Serializes the PageDirectoryHeader into a byte string.

        The byte string representation includes:
        - page_directory_id (int)
        - free_space_start (int)
        - page_size (int)
        - number of page pointers (int)
        - array of page pointers (each as page_id, page_start - both ints)

        Returns:
            bytes: The byte representation of the page directory header, padded t
                   page_size.
        """
        byte_str = pack(
            f"<iiii{len(self.page_pointers) * 2}i",
            self.page_directory_id,
            self.free_space_start,
            self.page_size,
            len(self.page_pointers),
            *chain(*self.page_pointers),
        )

        return byte_str.ljust(self.page_size, b"\0")

    @classmethod
    def from_bytes(
        cls, byte_str: bytes, file_path: Path, page_start_offset: int
    ) -> "PageDirectory":
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
            page_size,
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

        return PageDirectory(
            file_path,
            page_start_offset,
            page_directory_id,
            page_size,
            page_pointers,
            free_space_start,
        )

    @classmethod
    def from_file(
        cls, file_path: Path, page_directory_offset: int, page_directory_size_bytes: int
    ) -> "PageDirectory":
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
        with open(file_path, "rb") as f:
            f.seek(page_directory_offset)
            return PageDirectory.from_bytes(
                f.read(page_directory_size_bytes), file_path, page_directory_offset
            )

    def create_page(
        self, is_schema_page: bool = False
    ) -> SlottedPage | TableSchemaPage:  # TODO: create a page protocol.
        """
        Creates a new page (either SlottedPage or TableSchemaPage).

        This method instantiates a new page object, assigning it the next available
        page ID from the PageDirectory. The page is NOT immediately written to disk
        by this method. The caller is responsible for writing the page and updating
        the PageDirectory with the new page's location.

        Args:
            is_schema_page (bool, optional): If True, creates a TableSchemaPage;
                                              otherwise, creates a SlottedPage. #
                                              Defaults to False.

        Returns:
            SlottedPage | TableSchemaPage: The newly created page object.
                                             Returns TableSchemaPage if is_schema_page
                                             is True, otherwise returns a SlottedPage.
        """

        self.next_page_id += 1

        if is_schema_page:
            return TableSchemaPage(SlottedPage(self.next_page_id))
        else:
            return SlottedPage(self.next_page_id)

    def read_page(self, page_id: int) -> SlottedPage:
        """
        Reads a specific page from the database file.

        This method retrieves a page from disk based on its page ID. It looks up
        the page's offset in the PageDirectory's page pointers, seeks to that position
        in the database file, and deserializes the page data into a SlottedPage object.

        Args:
            page_id (int): The ID of the page to read.

        Returns:
            SlottedPage: The SlottedPage object read from disk.

        Raises:
            PageNotInDirectory: If the given page_id is not found in the PageDirectory.
        """
        try:
            page_offset = next(
                page.page_start
                for page in self.page_pointers
                if page.page_id == page_id
            )
            with open(self.file_path, "rb") as f:
                f.seek(self.page_start_offset + page_offset)
                return SlottedPage.from_bytes(f.read(self.page_size))

        except StopIteration:
            raise PageNotInDirectory

    def write_page(self, page: SlottedPage) -> None:
        """
        Writes a slotted page to the database file.

        This method writes the byte representation of a SlottedPage to disk.
        If the page is already tracked in the PageDirectory (i.e., it's an update),
        it writes to the existing location. If it's a new page (not found in
        PageDirectory), it appends the page to the end of the allocated RecordStorage
        space and updates the PageDirectory with a new PagePointer.

        Args:
            page (SlottedPage): The SlottedPage object to write to disk.

        Side Effects:
            - Updates the database file by writing page bytes.
            - May modify the PageDirectory's page_pointers list by adding a new
              PagePointer if the page_id was not previously known.
            - Updates the in-memory PageDirectoryHeader to reflect changes, and
              automatically persists these header changes to disk.

        """

        try:
            page_offset = next(
                stored_page.page_start
                for stored_page in self.page_pointers
                if page.page_id == stored_page.page_id
            )
        except StopIteration:
            page_offset = None

        with open(self.file_path, "r+b") as f:
            if not page_offset:
                # TODO: Think more smartly about this when we have deleted pages.
                page_offset = self.page_size * len(self.page_pointers)
                self.page_pointers.append(PagePointer(page.page_id, page_offset))
                f.seek(self.page_start_offset)
                f.write(bytes(self))
            f.seek(page_offset)
            f.write(page.byte_str)
