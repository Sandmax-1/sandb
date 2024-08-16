import logging
from collections import OrderedDict
from pathlib import Path
from typing import Any, Protocol, Tuple, TypeVar

from numpy import inf
from sortedcontainers import SortedDict

from db.config import ROOT_DIR

Comparable = TypeVar("Comparable", bound="ComparableType")
T = TypeVar("T")


class ComparableType(Protocol):
    """Protocol for annotating comparable types."""

    def __lt__(self: Comparable, other: Comparable) -> bool:
        pass


class LSMTree:
    def __init__(
        self, memtable_max_size: int = 1000, segment_chunk_size_for_indexing: int = 100
    ):
        self.memtable: SortedDict[Comparable, T] = SortedDict()
        self.memtable_max_size = memtable_max_size

        self.segment_chunk_size_for_indexing = segment_chunk_size_for_indexing
        # This is the SStable storage. First value is file path, second value is the
        # sparse index for the SStable. This is ordered such that we can look through
        # newest to oldest segments.
        self.segments: OrderedDict[Path, SortedDict] = OrderedDict()

        self.segment_index = 0

        self.segment_folder_path = ROOT_DIR / "lsm_segments"
        self.segment_folder_path.mkdir(exist_ok=True)

    def read_from_db(self, key: int) -> str | None:
        """
        First try and read from the in-memory memtable.
        If the key does not exist in there
        look at the segments stored on disk from latest to oldest.
        As the segments are stored
        in time order and an append of a new key counts
        as an update we can stop as soon as we find our key.

        Args:
            key (str): _description_

        Returns:
            Optional[int]: _description_
        """

        try:
            return str(self.memtable[key])

        except KeyError:
            logging.info(f"key: {key} not in in memory memtable")

        value = self.search_segments_on_disk(key)

        return value

    def search_segments_on_disk(self, key: int) -> str | None:
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
                    if ceil_offset and curr_offset >= ceil_offset:
                        value = ""
                        break

        return value

    def insert_into_db(self, key: Any, value: Any) -> None:
        if len(self.memtable) >= self.memtable_max_size:
            self.flush_memtable_to_disk()
        self.memtable.update({key: value})

    def flush_memtable_to_disk(self) -> None:
        index_counter = self.segment_chunk_size_for_indexing
        segment_file_name = (
            self.segment_folder_path / f"segment_{self.segment_index}.txt"
        )
        index = SortedDict()

        with open(segment_file_name, "a") as f:
            for key, value in self.memtable.items():
                if index_counter == 0:
                    offset = f.tell()
                    index.update({key: offset})
                    index_counter = self.segment_chunk_size_for_indexing

                f.write(f"{key}: {value}\n")
                index_counter -= 1

        self.memtable = SortedDict()
        self.segments.update({segment_file_name: index})
        self.segment_index += 1

    def get_floor_ceil_of_key_in_index(
        self, inputted_key: Comparable, index: SortedDict
    ) -> Tuple[int, int | None]:
        """
        Not very performant algorithm for looping through our SortedDict index
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
            value = int(value)
            if key == inputted_key:
                return (value, value)
            elif inputted_key < key:
                floor, ceil = prev_value, value
                break
            prev_value = value

        # Value either not in segment, or in final section.
        if ceil is None:
            floor = value

        return floor, ceil

    @staticmethod
    def turn_line_into_key_value(line: str) -> Tuple[int, str]:
        key_value = [x.strip() for x in line.split(":")]
        if len(key_value) != 2:
            raise Exception(
                f"""Line was parsed incorrectly.
                            Expected a key value pair seperated by a colon.
                            Received: {line}"""
            )
        return int(key_value[0]), key_value[1]

    # def compact_segment_files(
    #     self,
    #     segment_file_path_1: Path,
    #     segment_file_path_2: Path,
    #     compacted_file_path: Path,
    # ) -> Path:
    #     def process_next_line(
    #         segment_file: TextIOWrapper,
    #     ) -> Tuple[str, bool, Optional[str]]:
    #         line = segment_file.readline()
    #         if not line:
    #             return line, True, None
    #         key, _ = self.turn_line_into_key_value(line)
    #         return line, False, key

    #     with open(segment_file_path_1, "r") as segment_file_1, open(
    #         segment_file_path_2, "r"
    #     ) as segment_file_2, open(compacted_file_path, "a") as compacted_file:
    #         l1, file_1_empty, k1 = process_next_line(segment_file_1)
    #         l2, file_2_empty, k2 = process_next_line(segment_file_2)
    #         while not (file_1_empty or file_2_empty):
    #             if k1 < k2:
    #                 compacted_file.write(l1)
    #                 l1, file_1_empty, k1 = process_next_line(segment_file_1)

    #             elif k2 < k1:
    #                 compacted_file.write(l2)
    #                 l2, file_2_empty, k2 = process_next_line(segment_file_2)

    #             elif k1 == k2:
    #                 compacted_file.write(l2)
    #                 l1, file_1_empty, k1 = process_next_line(segment_file_1)
    #                 l2, file_2_empty, k2 = process_next_line(segment_file_2)

    #         remaining_file = segment_file_1 if not file_1_empty else segment_file_2
    #         final_line = l1 if not file_1_empty else l2

    #         compacted_file.write(final_line)
    #         for line in remaining_file.readlines():
    #             compacted_file.write(line)

    #     return compacted_file_path


def merge_segment_files(
    segment_file_paths: Tuple[Path, ...],
    merged_file_path: Path,
) -> Path:
    # TODO: Need to test whether this is actually more
    # efficient than merging two files over and over.

    def turn_line_into_key_value(line: str) -> Tuple[int, str]:
        key_value = [x.strip() for x in line.split(":")]
        if len(key_value) != 2:
            raise Exception(
                (
                    "Line was parsed incorrectly.\n"
                    "Expected a key value pair seperated by a colon.\n"
                    f"Received: {line}"
                )
            )
        return int(key_value[0]), key_value[1]

    with open(merged_file_path, "a") as output_file:
        try:
            segment_files = [file.open("r") for file in segment_file_paths]
            lines = [file.readline() for file in segment_files]
            keys = [turn_line_into_key_value(line)[0] if line else "" for line in lines]

            while any(keys):
                # This gets us the value we need to add to the merged file provided the
                # segment_files_list is in order of newest file to oldest.

                # TODO: This will only work with integer keys
                min_value_index = keys.index(
                    min(keys, key=lambda x: inf if x == "" else x)  # type: ignore
                )
                key = keys[min_value_index]

                output_file.write(lines[min_value_index])

                lines[min_value_index] = segment_files[min_value_index].readline()

                if lines[min_value_index]:
                    keys[min_value_index] = turn_line_into_key_value(
                        lines[min_value_index]
                    )[0]
                else:
                    keys[min_value_index] = ""

                breakout = False

                while not breakout:
                    try:
                        next_index = keys.index(key)
                        lines[next_index] = segment_files[next_index].readline()
                        if lines[next_index]:
                            keys[next_index] = turn_line_into_key_value(
                                lines[next_index]
                            )[0]
                        else:
                            keys[next_index] = ""

                    except ValueError:
                        breakout = True
        except Exception as e:
            raise e
        finally:
            for file in segment_files:
                file.close()
    return merged_file_path
