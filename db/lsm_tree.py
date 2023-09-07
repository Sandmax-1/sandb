from dataclasses import dataclass
from typing import List, NamedTuple, Optional, Tuple
from red_black_dict_mod import RedBlackTree
import sys
import logging
from pathlib import Path
from collections import OrderedDict

from db.config import ROOT_DIR

@dataclass()
class IndexValue():
    segment_index: int
    key: str
    offset: int


class LSMTree:
    def __init__(
        self, memtable_max_size: int = 1000, segment_chunk_size_for_indexing: int = 100
    ):
        self.memtable: RedBlackTree = RedBlackTree()
        self.memtable_max_size = memtable_max_size

        self.segment_chunk_size_for_indexing = segment_chunk_size_for_indexing
        # This is the SStable storage. First value is file path, second value is the
        # sparse index for the SStable. This is ordered such that we can look through
        # newest to oldest segments.
        self.segments: OrderedDict[Path, RedBlackTree] = OrderedDict()

        self.segment_index = 0

        self.segment_folder_path = ROOT_DIR / "lsm_segments"
        self.segment_folder_path.mkdir(exist_ok=True)

    def read_from_db(self, key: str) -> Optional[int]:
        """
        First try and read from the in-memory memtable. If the key does not exist in there
        look at the segments stored on disk from latest to oldest. As the segments are stored
        in time order and an append of a new key counts as an update we can stop as soon as
        we find our key.

        Args:
            key (str): _description_

        Returns:
            Optional[int]: _description_
        """

        try:
            value = self.memtable[key]
            return value
        except KeyError:
            logging.info(f"key: {key} not in in memory memtable")

        value = self.search_segments_on_disk(key)

        return value

    def search_segments_on_disk(self, key: str) -> Optional[int]:
        for filepath, index in self.segments.items():
            floor_offset, ceil_offset = self.get_floor_ceil_of_key_in_index(key, index)
            with open(filepath, "r") as current_segment:
                current_segment.seek(floor_offset)
                curr_offset = floor_offset
                for line in current_segment:
                    curr_offset += len(line) 
                    stored_key, value = line.split(":")
                    if stored_key == str(key):
                        return value.strip()
                    if curr_offset >= ceil_offset:
                        value = None
                        break

        return value

    def insert_into_db(self, key: str, value: int) -> None:
        if len(self.memtable) >= self.memtable_max_size:
            self.flush_memtable_to_disk()
        self.memtable.add(key, value)

    def flush_memtable_to_disk(self):
        index_counter = self.segment_chunk_size_for_indexing
        segment_file_name = (
            self.segment_folder_path / f"segment_{self.segment_index}.txt"
        )
        index = RedBlackTree()

        with open(segment_file_name, "a") as f:
            for key, value in self.memtable.items():
                if index_counter == 0:
                    offset = f.tell()
                    index.add(
                        key, offset
                    )
                    index_counter = self.segment_chunk_size_for_indexing

                f.write(f"{key}: {value}\n")
                index_counter -= 1

        self.memtable = RedBlackTree()
        self.segments.update({segment_file_name: index})
        self.segment_index += 1

    def get_floor_ceil_of_key_in_index(
        self, inputted_key: str, index: RedBlackTree
    ) -> Tuple[int, int]:
        """
        Not very performant algorithm for looping through our red black tree index
        to find the boundary where the key to search for is.

        I.e. if our tree looks something like:
        {a: 100
        h: 200
        q: 300
        z: 400}
        then the boundaries if we try and find key j would be a and h.
        """
        floor = 0
        ceil = None
        prev_value = 0
        for key, value in index.items():
            if key == inputted_key:
                return (value, value)
            elif inputted_key < key:
                floor, ceil = prev_value, value
                break
            prev_value = value

        if ceil is None:
            floor, ceil = value, None

        return floor, ceil


countries_dict = {
    "Germany": "Berlin",
    "Hungary": "Budapest",
    "Ireland": "Dublin",
    "Portugal": "Lisbon",
    "Cyprus": "Nicosia",
    "Greenland": "Nuuk",
    "Iceland": "Reykjavik",
    "Macedonia": "Skopje",
    "Bulgaria": "Sofia",
    "Sweden": "Stockholm",
}
a = RedBlackTree()

for key, value in countries_dict.items():
    a.add(key, value)

print(sys.getsizeof(a))
