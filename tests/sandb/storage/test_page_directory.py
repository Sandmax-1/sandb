from pathlib import Path
from tempfile import TemporaryDirectory

from sandb.storage.page_directory import PageDirectoryHeader, PagePointer


def test_to_binary():
    with TemporaryDirectory() as tmp:
        file_path = Path(tmp) / "page_directory"
        file_path.touch()
        page = PageDirectoryHeader(1, 2, 3, [PagePointer(1, 2), PagePointer(3, 4)])

        page.to_binary(file_path, 0)

        assert PageDirectoryHeader.from_binary(file_path, 0) == page
