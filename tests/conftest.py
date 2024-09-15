import pytest
from pytest import TempPathFactory
from sortedcontainers import SortedDict

from sandb.tables.metadata import Column, TableMetadata


@pytest.fixture  # type: ignore
def test_tree() -> SortedDict[str, int]:
    countries_dict = SortedDict(
        {
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
    )

    return countries_dict


@pytest.fixture  # type: ignore
def test_table_metadata(tmp_path_factory: TempPathFactory) -> TableMetadata:
    path = tmp_path_factory.mktemp("tables")
    return TableMetadata(
        name="test_table",
        columns=(Column(name="col_1", dtype=str), Column(name="col_2", dtype=int)),
        location=path,
    )
