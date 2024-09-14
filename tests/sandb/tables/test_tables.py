import pytest
from pydantic_core import from_json

from sandb.tables.metadata import TableMetadata
from sandb.tables.table import TableExistsError, create


def test_create_happy_path(test_table_metadata: TableMetadata) -> None:
    create(test_table_metadata)

    with open(
        test_table_metadata.location / test_table_metadata.name / "metadata.json"
    ) as f:
        saved_metadata = f.read()

    actual_metadata = TableMetadata.model_validate_json(
        from_json(saved_metadata, allow_partial=True)
    )

    assert actual_metadata == test_table_metadata


def test_create_table_folder_already_exists(test_table_metadata: TableMetadata) -> None:
    (test_table_metadata.location / test_table_metadata.name).mkdir()

    with pytest.raises(TableExistsError):
        create(test_table_metadata)
