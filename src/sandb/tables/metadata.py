from pathlib import Path

from pydantic import BaseModel

from sandb.indexes.abc import Index

VALID_DTYPES = str | int


class Column(BaseModel):
    name: str
    dtype: VALID_DTYPES


class TableMetadata(BaseModel):
    name: str
    columns: tuple[Column]
    location: Path
    indexes: None | tuple[Index]
