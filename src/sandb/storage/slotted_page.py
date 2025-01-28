from dataclasses import dataclass


@dataclass
class Slot:
    cell_id: int
    cell_pointer: int
    cell_length: int


@dataclass
class SlottedPageHeader:
    page_id: int
    free_space_start: int
    free_space_end: int
    slots: list[Slot]
    checksum: int | None  # TODO implement this later
