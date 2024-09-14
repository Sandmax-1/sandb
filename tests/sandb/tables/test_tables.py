from pydantic_core import from_json

from sandb.tables.metadata import TableMetadata
from sandb.tables.table import create


def test_create(test_table_metadata: TableMetadata) -> None:
    create(test_table_metadata)

    with open(
        test_table_metadata.location / test_table_metadata.name / "metadata.json"
    ) as f:
        saved_metadata = f.read()

    actual_metadata = TableMetadata.model_validate_json(
        from_json(saved_metadata, allow_partial=True)
    )

    assert actual_metadata == test_table_metadata
