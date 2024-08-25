from pathlib import Path

from pydantic import BaseModel

from sandb.indexes.abc import Index

VALID_DTYPES = str | int


class Column(BaseModel):
    """
    Holds the metadata about a column. Only needs column name and dtype.
    dtype only allowed to come from VALID_DTYPES list.
    """

    name: str
    dtype: VALID_DTYPES


class TableMetadata(BaseModel):
    """
    Holds metadata about a given table.
    """

    name: str
    columns: tuple[Column, ...]
    location: Path
    indexes: None | tuple[Index, ...]
