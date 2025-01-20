import json
import logging
from collections import OrderedDict
from glob import glob
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Tuple

from numpy import inf
from sortedcontainers import SortedDict

from sandb.config import VALID_DTYPE
from sandb.indexes.abc import Index
from sandb.tables.metadata import LSMTreeMetadata


class LSMTree(Index):
    def __init__(self, lsmtree_metadata: LSMTreeMetadata):
        self.memtable: dict[VALID_DTYPE, Any] = SortedDict()

        self.metadata = lsmtree_metadata

        # This is the SStable storage. First value is file path, second value is the
        # sparse index for the SStable. This is ordered such that we can look through
        # newest to oldest segments.
        self.indexes: OrderedDict[Path, dict[VALID_DTYPE, int]]

        if self.metadata.metadata_file_path.exists():
            self.indexes = self._load_indexes_from_file()

        else:
            with open(self.metadata.metadata_file_path, "w") as f:
                json.dump(self.metadata.model_dump_json(), f)
            self.indexes = OrderedDict()
            self.metadata.segment_folder_path.mkdir()

        self.segment_index = len(self.indexes)

    def read(self, key: VALID_DTYPE) -> str | None:
        """
        First try and read from the in-memory memtable.
        If the key does not exist in there
        look at the segments stored on disk from latest to oldest.
        As the segments are stored in time order and an append of a
        new key counts as an update we can stop as soon as we find our key.

        Args:
            key (str): _description_

        Returns:
            Optional[int]: _description_
        """

        try:
            return str(self.memtable[key])

        except KeyError:
            logging.info(f"key: {key} not in in memory memtable")

        value = self._search_segments_on_disk(key)

        return value

    def write(self, key: VALID_DTYPE, value: Any) -> None:
        if len(self.memtable) >= self.metadata.memtable_max_size:
            self._flush_memtable_to_disk()
        self.memtable.update({key: value})

    def _load_indexes_from_file(self) -> OrderedDict[Path, dict[VALID_DTYPE, int]]:
        number_of_segments = len(glob(str(self.metadata.segment_folder_path / "*")))
        with open(self.metadata.index_file_path, "r") as f:
            indexes = f.read()

        individual_indexes = indexes.split("\n\n")[:-1]

        if number_of_segments != len(individual_indexes):
            raise Exception(
                "We have an incompatible number of indexes to segment files"
            )

        split_indexes = [index.split("\n") for index in individual_indexes]

        serialised_indexes: list[dict[VALID_DTYPE, int]] = []
        for index in split_indexes:
            serialised_indexes.append(
                SortedDict(
                    {
                        self.metadata.primary_key.dtype(row.split(":")[0]): int(
                            row.split(":")[1]
                        )
                        for row in index
                    }
                )
            )

        return OrderedDict(
            zip(
                [
                    self.metadata.segment_folder_path / f"segment_{ind}.txt"
                    for ind in range(number_of_segments)
                ],
                serialised_indexes,
            )
        )

    def _search_segments_on_disk(self, key: VALID_DTYPE) -> str | None:
        value = ""
        for filepath, index in self.indexes.items():
            floor_offset, ceil_offset = self._get_floor_ceil_of_key_in_index(key, index)
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

    def _flush_memtable_to_disk(self) -> None:
        index_counter = self.metadata.segment_chunk_size_for_indexing
        segment_file_path = (
            self.metadata.segment_folder_path / f"segment_{self.segment_index}.txt"
        )
        index: dict[VALID_DTYPE, int] = SortedDict()

        with open(segment_file_path, "a") as f:
            for key, value in self.memtable.items():
                if index_counter == 0:
                    offset = f.tell()
                    index.update({key: offset})
                    index_counter = self.metadata.segment_chunk_size_for_indexing

                f.write(f"{key}: {value}\n")
                index_counter -= 1

        self.memtable = SortedDict()
        self.indexes.update({segment_file_path: index})
        self.segment_index += 1

    def _get_floor_ceil_of_key_in_index(
        self, inputted_key: VALID_DTYPE, index: dict[VALID_DTYPE, int]
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

        Returns:
            tuple[int, int | None] where the first element is the start of the area to
            search on file for our value and the second element is the end, or
            None if key might be in the last segment.
        """
        # TODO: update this to use binary search
        floor = 0
        ceil = None
        prev_value = 0
        value = 0
        for key, value in index.items():
            value = int(value)
            if type(inputted_key) is not type(key):
                raise ValueError(f"Type mismatch: {type(inputted_key)} != {type(key)}")
            elif key == inputted_key:
                return (value, value)
            elif inputted_key < key:  # type: ignore
                floor, ceil = prev_value, value
                break
            prev_value = value

        # Value either not in segment, or in final section.
        if ceil is None:
            floor = value

        return floor, ceil


def save_index_to_file(folder_path: Path, index: dict[VALID_DTYPE, int]) -> None:
    """
    Saves our indexes which are used to efficiently scan our segments on file.
    Currently just uses a simple txt format where each item in the index is stored
    as a str like 'key':'value'. It will then append a newline character so that
    we can determine when one index is finished and the next begins. Currently relies
    on the order the indexes are saved to disk which correspond to the order of the
    segment files.

    Args:
        index (dict[VALID_DTYPE, int]): index to save to file
    """

    filepath = folder_path / "index.txt"

    if not filepath.exists():
        filepath.touch()

    with open(filepath, "a") as f:
        for key, value in index.items():
            f.write(str(key) + ":" + str(value) + "\n")

        f.write("\n")


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

    segment_files: list[
        TextIOWrapper
    ] = []  # Initialising to avoid possible unbound errors in finally block
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
                    min(keys, key=lambda x: inf if x == "" else x)
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
