from typing import NamedTuple
from red_black_dict_mod import RedBlackTree
import sys

from config import ROOT_DIR


class IndexValue(NamedTuple):
    segment_index: int
    key: str
    offset: int


class LSMTree:
    def __init__(self, memtable_max_size: int = 1000):
        self.memtable = RedBlackTree()
        self.memtable_max_size = memtable_max_size

        self.index = RedBlackTree()
        self.segment_chunk_size_for_indexing = 100

        self.segment_index = 0

        self.segment_folder_path = ROOT_DIR / "lsm_segments"
        self.segment_folder_path.mkdir(exist_ok=True)

    def insert_into_db(self, key: str, value: int) -> None:
        self.memtable.add(key, value)
        if sys.getsizeof(self.memtable) > self.memtable_max_size:
            self.save_memtable_to_disk()

    def save_memtable_to_disk(self):
        index_counter = self.segment_chunk_size_for_indexing
        offset = 0
        segment_file_name = (
            self.segment_folder_path / f"segment_{self.segment_index}.txt"
        )

        with open(segment_file_name, "a") as f:
            for key, value in self.memtable.items:
                if index_counter == 0:
                    self.index.add(
                        IndexValue(
                            segment_index=self.segment_index, key=key, offset=offset
                        )
                    )
                    index_counter = self.segment_chunk_size_for_indexing

                record = f"{key}: {value}\n"
                f.write(f"{key}: {value}\n")
                offset += len(record)
                index_counter -= 1

        self.segment_index += 1


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
