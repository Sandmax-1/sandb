from pathlib import Path
from tempfile import TemporaryDirectory

from sandb.storage.constants import CHAR_SIZE_IN_BYTES, INT_SIZE_IN_BYTES
from sandb.storage.database import Database


def test_to_binary() -> None:
    with TemporaryDirectory() as tmp:
        file_path = Path(tmp) / "database"
        file_path.touch()

        database = Database(
            file_path, "v0.0.1", 4, 6 * CHAR_SIZE_IN_BYTES + 2 * INT_SIZE_IN_BYTES
        )

        database.to_binary()

        assert Database.from_binary(file_path) == database
