from dataclasses import dataclass

from sandb.storage.record import SchemaRecord
from sandb.storage.slotted_page import SlottedPage


@dataclass
class TableSchemaPage:
    _page: SlottedPage  # TODO: make this a property

    def add_record(self, schema_record: SchemaRecord) -> int:
        return self._page.add_record(bytes(schema_record))

    def delete(self, record_id: int) -> None:
        self._page.delete(record_id)

    def update(self, record_id: int, schema_record: SchemaRecord) -> int:
        return self._page.update(record_id, bytes(schema_record))

    def get_record(self, record_id: int) -> SchemaRecord:
        return SchemaRecord.from_bytes(self._page.get_record(record_id))

    def __bytes__(self) -> bytes:
        return bytes(self._page.byte_str)

    @property
    def page_id(self) -> int:
        return self._page.page_id
