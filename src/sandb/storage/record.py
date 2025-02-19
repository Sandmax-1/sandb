from dataclasses import dataclass
from itertools import chain
from struct import pack, unpack_from
from typing import Literal, TypeAlias

# Need to ensure these are in the same order.
ValidDTypes: TypeAlias = type[int] | type[str]
ValidDTypeAliases: TypeAlias = Literal[0, 1]

VALID_DTYPES_MAPPING: dict[ValidDTypeAliases, ValidDTypes] = {0: int, 1: str}

REVERSED_VALID_DTYPES_MAPPING: dict[ValidDTypes, int] = {
    v: k for k, v in VALID_DTYPES_MAPPING.items()
}


@dataclass
class SchemaRecord:
    """
    Represents a schema record containing table metadata, including column names
    and data types.

    Attributes:
        table_id (int): Unique identifier for the table.
        table_name (str): Name of the table.
        dtypes (list[ValidDTypeAliases]): List of data type aliases for each column.
        col_names (list[str]): List of column names in the table.
    """

    table_id: int
    table_name: str
    dtypes: list[
        ValidDTypeAliases
    ]  # TODO: need to add dtype lengths in here as well i.e. VARCHAR(50)
    col_names: list[str]

    def __bytes__(self) -> bytes:
        """
        Serializes the SchemaRecord instance into a bytes representation.

        Returns:
            bytes: Serialized binary representation of the schema record.
        """
        byte_str = pack(
            f"<iii{len(self.table_name)}s{len(self.dtypes)}i",
            self.table_id,
            len(self.dtypes),
            len(self.table_name),
            self.table_name.encode("utf-8"),
            *self.dtypes,
        )

        byte_format_col_names = "<" + "".join(
            [f"i{len(name)}s" for name in self.col_names]
        )

        col_names_byte_str = pack(
            byte_format_col_names,
            *chain(*[(len(name), name.encode("utf-8")) for name in self.col_names]),
        )

        return byte_str + col_names_byte_str

    @classmethod
    def from_bytes(cls, byte_str: bytes) -> "SchemaRecord":
        """
        Deserializes a byte string into a SchemaRecord instance.

        Args:
            byte_str (bytes): The binary data representing a serialized schema record.

        Returns:
            SchemaRecord: The deserialized schema record object.
        """
        offset = 0
        table_id, num_cols, table_name_length = unpack_from("<iii", byte_str, offset)
        offset += 12

        table_name = unpack_from(f"<{table_name_length}s", byte_str, offset)[0].decode(
            "utf-8"
        )
        offset += table_name_length

        dtypes = list(unpack_from(f"<{num_cols}i", byte_str, offset))
        offset += num_cols * 4

        col_names: list[str] = []
        while offset < len(byte_str):
            col_name_length = unpack_from("<i", byte_str, offset)[0]
            offset += 4
            col_name = unpack_from(f"<{col_name_length}s", byte_str, offset)[0].decode(
                "utf-8"
            )
            offset += col_name_length
            col_names.append(col_name)

        return cls(table_id, table_name, dtypes, col_names)
