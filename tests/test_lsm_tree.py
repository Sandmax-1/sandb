import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from num2words import num2words
from red_black_dict_mod import RedBlackTree

from db.config import ROOT_DIR
from db.lsm_tree import LSMTree, merge_segment_files

import glob


@pytest.fixture()
def test_tree():
    countries_dict = {
        "Bulgaria": 10,
        "Cyprus": 20,
        "Germany": 30,
        "Greenland": 40,
        "Hungary": 50,
        "Iceland": 60,
        "Ireland": 70,
        "Macedonia": 80,
        "Portugal": 90,
        "Sweden": 100,
    }

    test_tree = RedBlackTree()

    for key, value in countries_dict.items():
        test_tree.add(key, value)

    return test_tree


@pytest.mark.parametrize(
    argnames=["input_key", "expected_output"],
    argvalues=[
        ("Andorra", (0, 10)),
        ("England", (20, 30)),
        ("Zimbabwe", (100, None)),
        ("Cyprus", (20, 20)),
    ],
    ids=[
        "Outside begining of tree",
        "In the middle of the tree",
        "Past the end of the tree",
        "Hit a key",
    ],
)
def test_get_floor_ceil_of_key_in_index(input_key, expected_output, test_tree):
    lsm = LSMTree()
    assert lsm.get_floor_ceil_of_key_in_index(input_key, test_tree) == expected_output


LIST_OF_NUMS = [
    0,
    10,
    97,
    44,
    5,
    11,
    58,
    41,
    97,
    54,
    90,
    39,
    54,
    11,
    28,
    89,
    1,
    54,
    25,
    74,
]


def test_read_from_db():
    with TemporaryDirectory(dir=ROOT_DIR) as tmp:
        lsmtree = LSMTree(10, 3)
        lsmtree.segment_folder_path = Path(tmp)
        for num in LIST_OF_NUMS:
            lsmtree.insert_into_db(num, num2words(num))

        assert lsmtree.read_from_db(10) == "ten"
        assert lsmtree.read_from_db(3) is None


LONGER_LIST_OF_NUMS = [
    506,
    232,
    568,
    496,
    317,
    840,
    459,
    663,
    704,
    943,
    713,
    315,
    694,
    609,
    654,
    537,
    55,
    357,
    706,
    583,
    775,
    382,
    629,
    545,
    267,
    164,
    901,
    49,
    257,
    839,
    580,
    925,
    560,
    53,
    468,
    357,
    141,
    834,
    98,
    451,
    278,
    126,
    906,
    312,
    661,
    383,
    420,
    988,
    28,
    816,
    678,
    632,
    986,
    511,
    368,
    244,
    688,
    799,
    83,
    191,
    131,
    869,
    896,
    100,
    19,
    206,
    461,
    731,
    184,
    435,
    887,
    798,
    880,
    333,
    677,
    880,
    997,
    397,
    856,
    489,
    196,
    561,
    693,
    909,
    158,
    455,
    277,
    153,
    581,
    768,
    31,
    124,
    179,
    608,
    198,
    820,
    562,
    460,
    284,
    601,
]


def test_write_to_db():
    # Expect 3 files and a memtable with 25 els. as Dupes are in different segments
    with TemporaryDirectory(dir=ROOT_DIR) as tmp:
        lsmtree = LSMTree(25, 5)
        lsmtree.segment_folder_path = Path(tmp)
        for num in LONGER_LIST_OF_NUMS:
            lsmtree.insert_into_db(num, num2words(num))

        assert os.listdir(tmp) == ["segment_0.txt", "segment_1.txt", "segment_2.txt"]
        assert len(lsmtree.memtable) == 25


@pytest.mark.parametrize(
    argnames=["file_contents", "expected_merged_file_contents"],
    ids=[
        "Original 2 file test",
        "3 files",
        "normal file plus empty file",
        "only an empty file",
    ],
    argvalues=[
        (
            [[1, 2, 3, 4, 5], [1, 2, 4, 7, 8]],
            [
                "1: one_2\n",
                "2: two_2\n",
                "3: three_1\n",
                "4: four_2\n",
                "5: five_1\n",
                "7: seven_2\n",
                "8: eight_2\n",
            ],
        ),
        (
            [[1, 2, 3, 4, 5], [1, 2, 4, 7, 8], [2, 4, 6, 8]],
            [
                "1: one_2\n",
                "2: two_3\n",
                "3: three_1\n",
                "4: four_3\n",
                "5: five_1\n",
                "6: six_3\n",
                "7: seven_2\n",
                "8: eight_3\n",
            ],
        ),
        (
            [[1, 2, 3], []],
            [
                "1: one_1\n",
                "2: two_1\n",
                "3: three_1\n",
            ],
        ),
        (
            [[]],
            [],
        ),
    ],
)
def test_compact_segment_files(
    file_contents: list[list[int]], expected_merged_file_contents: list[str]
):
    filepaths = []
    with TemporaryDirectory(dir=ROOT_DIR) as tmp:
        for index, contents in enumerate(file_contents):
            filepath = Path(tmp) / f"segment_{index}.txt"
            filepaths.append(filepath)
            with open(filepath, "a") as f:
                for val in contents:
                    f.write(f"{val}: {num2words(val)}_{index + 1}\n")

        output_filepath = merge_segment_files(
            reversed(filepaths), Path(tmp) / "output_file.txt"
        )

        with open(output_filepath, "r") as f:
            actual = list(f.readlines())

        assert actual == expected_merged_file_contents
