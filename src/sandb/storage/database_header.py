from dataclasses import dataclass, field
from struct import pack, unpack_from

from sandb.storage.constants import INT_SIZE_IN_BYTES


@dataclass
class DatabaseHeader:
    """
    Represents a database stored in a file with metadata about its version, page size,
    and page directory.

    Attributes:
        version (str): Version of the database.
        page_size_bytes (int): Size of each page in bytes.
        page_directory_size_bytes (int): Size of the page directory in bytes.
        page_directory_start (int): Byte offset where the page directory starts
                                    in the file.
    """

    version: str
    page_size_bytes: int
    page_directory_start: int = field(init=False)

    def __post_init__(self) -> None:
        """
        Initializes the page directory start offset based on the version string length
        and integer size in bytes.
        """
        self.page_directory_start = (3 * INT_SIZE_IN_BYTES) + len(self.version)

    @classmethod
    def from_bytes(cls, byte_str: bytes) -> "DatabaseHeader":
        """
        Creates a DatabaseHeader instance from a byte string.

        Args:
            byte_str (bytes): Byte string representing the database metadata.
            file_path (Path): Path to the database file.

        Returns:
            Database: A new Database instance populated with data from the byte string.
        """
        offset = 0

        version_str_len = unpack_from("<i", byte_str, offset)[0]
        offset += 4
        version_str = unpack_from("<" + f"{version_str_len}s", byte_str, offset)[
            0
        ].decode(encoding="utf-8")
        offset += version_str_len
        page_size_bytes = unpack_from("<i", byte_str, offset)[0]

        return DatabaseHeader(
            str(version_str),
            page_size_bytes,
        )

    def __bytes__(self) -> bytes:
        """
        Serializes the DatabaseHeader instance into a byte string.

        Returns:
            bytes: A byte representation of the database metadata.
        """
        return pack(
            f"<i{len(self.version)}si",
            len(self.version),
            self.version.encode("utf-8"),
            self.page_size_bytes,
        )
