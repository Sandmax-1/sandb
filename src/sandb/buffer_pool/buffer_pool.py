from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock

from sandb.storage.page_directory import PageDirectory
from sandb.storage.slotted_page import SlottedPage


@dataclass
class BufferPageMetadata:
    frame_index: int
    pin: Lock


@dataclass
class PageTable:
    buffer_pool_page_mapping: dict[int, BufferPageMetadata] = field(
        default_factory=dict
    )


@dataclass
class BufferPool:
    frames: list[SlottedPage] = field(default_factory=list)
    
    def __len__(self) -> int:
        return len(self.frames)


class BufferPoolManager:
    def __init__(self, buffer_pool_size: int, page_directory: PageDirectory):
        self.page_table = PageTable()
        self.buffer_pool = BufferPool()
        self.buffer_pool_size = buffer_pool_size
        self.page_directory = page_directory
        
    def is_full(self) -> bool:
        return len(self.buffer_pool) == self.buffer_pool_size 

    def get_page(self, page_id: int) -> SlottedPage | None:
        buffer_page_metadata = self.page_table.buffer_pool_page_mapping.get(
            page_id, None
        )

        if buffer_page_metadata:
            buffer_page_metadata.pin.acquire()
            self.page_table.buffer_pool_page_mapping[page_id] = buffer_page_metadata
            page = self.buffer_pool.frames[buffer_page_metadata.frame_index]
            return page

        else:
            page = self.page_directory.read_page(page_id)
            return page

    def release_page(self, page_id: int) -> None:
        buffer_page_metadata = self.page_table.buffer_pool_page_mapping.get(
            page_id, None
        )
        if buffer_page_metadata:
            buffer_page_metadata.pin.release()

        else:
            raise Exception(
                f"page: {page_id} not in buffer pool so can't release lock."
            )
