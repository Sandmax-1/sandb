from dataclasses import dataclass


@dataclass
class PageDirectoryHeader:
    page_directory_id: int
    free_space_start: int
    directory_size: int
    page_sizes: int


@dataclass
class PagePointer:
    page_id: int
    page_start: int
