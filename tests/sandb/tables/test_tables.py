import json

import pytest

from sandb.tables.metadata import TableMetadata
from sandb.tables.table import TableExistsError, create


def test_create_happy_path(test_table_metadata: TableMetadata) -> None:
    create(test_table_metadata)

    with open(
        test_table_metadata.location / test_table_metadata.name / "metadata.json"
    ) as f:
        saved_metadata = json.load(f)

    assert saved_metadata == test_table_metadata.model_dump_json()


def test_create_table_folder_already_exists(test_table_metadata: TableMetadata) -> None:
    (test_table_metadata.location / test_table_metadata.name).mkdir()

    with pytest.raises(TableExistsError):
        create(test_table_metadata)
