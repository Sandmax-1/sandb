from sandb.storage.record import SchemaRecord


def test_schema_record_binary_storage() -> None:
    schema_record = SchemaRecord(0, "table_1", [0, 1], col_names=["col_1", "col_2"])

    assert schema_record == schema_record.from_bytes(bytes(schema_record))
