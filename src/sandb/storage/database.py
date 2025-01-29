from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from struct import pack, unpack

from sandb.storage.constants import INT_SIZE_IN_BYTES
from sandb.storage.page_directory import PageDirectoryHeader


@dataclass
class Database:
    file_path: Path
    version: str
    page_size_bytes: int
    page_directory_size_bytes: int
    page_directory_start: int

    @classmethod
    def from_binary(cls, file_path: Path) -> "Database":
        with open(file_path, "rb") as f:
            version_str_len = unpack("<i", f.read(INT_SIZE_IN_BYTES))[0]
            version_str = unpack("<" + f"{version_str_len}s", f.read(version_str_len))[
                0
            ].decode(encoding="utf-8")
            page_size_bytes, page_directory_size = unpack(
                "<ii", f.read(2 * INT_SIZE_IN_BYTES)
            )

        return Database(
            file_path,
            str(version_str),
            page_size_bytes,
            page_directory_size,
            (3 * INT_SIZE_IN_BYTES) + version_str_len,
        )

    def to_binary(self) -> None:
        with open(self.file_path, "ab") as f:
            f.write(pack("<i", len(self.version)))
            f.write(
                pack(
                    "<" + f"{len(self.version)}s", bytes(self.version, encoding="utf-8")
                )
            )
            f.write(pack("<ii", self.page_size_bytes, self.page_directory_size_bytes))

    @cached_property
    def page_directory(self) -> PageDirectoryHeader:
        return PageDirectoryHeader.from_binary(
            self.file_path, self.page_directory_start
        )
