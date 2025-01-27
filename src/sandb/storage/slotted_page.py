from dataclasses import dataclass


@dataclass
class SlottedPageHeader:
    page_id: int
    free_space_start: int
    free_space_end: int
    checksum: int | None  # TODO implement this later


@dataclass
class Slot:
    cell_pointer: int
    cell_length: int
