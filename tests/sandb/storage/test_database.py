from pathlib import Path
from tempfile import TemporaryDirectory

from sandb.storage.database import Database
from sandb.storage.page_directory import PageDirectoryHeader, PagePointer


def test_to_bytes() -> None:
    database = Database(
        file_path=Path("abc"),
        version="v0.0.1",
        page_size_bytes=1000,
        page_directory_size_bytes=100,
    )

    encoded_database = database.to_bytes()

    assert Database.from_bytes(encoded_database, Path("abc")) == database


def test_database_page_directory_cohesion() -> None:
    with TemporaryDirectory() as tmp:
        file_path = Path(tmp) / "database"
        file_path.touch()

        database = Database(
            file_path=file_path,
            version="v0.0.1",
            page_size_bytes=1000,
            page_directory_size_bytes=100,
        )

        page = PageDirectoryHeader(1, 2, 3, [PagePointer(1, 2), PagePointer(3, 4)])

        with open(file_path, "wb") as f:
            f.write(database.to_bytes())
            f.write(page.to_bytes())

        assert database.page_directory == page
