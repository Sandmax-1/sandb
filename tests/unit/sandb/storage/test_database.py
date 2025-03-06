from sandb.storage.database_header import DatabaseHeader


def test_to_bytes() -> None:
    database = DatabaseHeader(
        version="v0.0.1",
        page_size_bytes=1000,
    )

    encoded_database = bytes(database)

    assert DatabaseHeader.from_bytes(encoded_database) == database
