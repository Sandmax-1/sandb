import os
from pathlib import Path

from sandb.buffer_pool.buffer_pool import BufferPoolManager
from sandb.storage.database_header import DatabaseHeader
from sandb.storage.page_directory import PageDirectory


class Database:
    def __init__(
        self, file_path: Path, buffer_pool_size: int, page_size: int = 4096
    ) -> None:
        self.file_path = file_path
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                # TODO: find better way to load header rather than reading 50 bytes.
                self.database_header = DatabaseHeader.from_bytes(f.read(50))
                f.seek(self.database_header.page_directory_start)
                page_directory = PageDirectory.from_bytes(
                    f.read(page_size),
                    file_path,
                    self.database_header.page_directory_start,
                )
        else:
            with open(file_path, "wb") as f:
                self.database_header = DatabaseHeader(
                    version="v0.0.1",
                    page_size_bytes=4096,
                )
                f.write(bytes(self.database_header))
                page_directory = PageDirectory(
                    file_path=file_path,
                    page_start_offset=self.database_header.page_directory_start,
                    page_directory_id=0,
                    page_size=4096,
                )
                f.write(bytes(page_directory))

        self.buffer_pool = BufferPoolManager(buffer_pool_size, page_directory)
