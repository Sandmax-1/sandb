from lsm_tree import LSMTree
import pytest
from red_black_dict_mod import RedBlackTree


@pytest.fixture()
def test_tree():
    countries_dict = {
        "Bulgaria": "Sofia",
        "Cyprus": "Nicosia",
        "Germany": "Berlin",
        "Greenland": "Nuuk",
        "Hungary": "Budapest",
        "Iceland": "Reykjavik",
        "Ireland": "Dublin",
        "Macedonia": "Skopje",
        "Portugal": "Lisbon",
        "Sweden": "Stockholm",
    }

    test_tree = RedBlackTree()

    for key, value in countries_dict.items():
        test_tree.add(key, value)

    return test_tree


@pytest.mark.parametrize(
    argnames=["input_key", "expected_output"],
    argvalues=[
        ("Andorra", (None, "Sofia")),
        ("England", ("Nicosia", "Berlin")),
        ("Zimbabwe", ("Stockholm", None)),
        ('Cyprus', ('Nicosia', 'Nicosia'))
    ],
    ids=[
        "Outside begining of tree",
        "In the middle of the tree",
        "Past the end of the tree",
        "Hit a key"
    ],
)
def test_get_floor_ceil_of_key_in_index(input_key, expected_output, test_tree):
    lsm = LSMTree()
    lsm.index = test_tree
    assert lsm.get_floor_ceil_of_key_in_index(input_key) == expected_output
