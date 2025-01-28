from dataclasses import dataclass
from functools import cached_property
from pathlib import Path

from sandb.storage.page_directory import PageDirectoryHeader


@dataclass
class Database:
    file_path: Path
    version: str
    page_size: int

    @classmethod
    def from_binary(cls, file_path: Path) -> "Database":
        pass

    @cached_property
    def page_directory(self) -> PageDirectoryHeader:
        return PageDirectoryHeader.from_binary(self.file_path, 16)
