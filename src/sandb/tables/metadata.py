from pathlib import Path
from typing import Any, Literal, get_args

from pydantic import BaseModel, ConfigDict, Field

from sandb.indexes.abc import Index

VALID_DTYPES = Literal[0, 1]
VALID_DTYPES_MAPPING: dict[VALID_DTYPES, Any] = dict(
    zip(get_args(VALID_DTYPES), (str, int))
)


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

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    columns: tuple[Column, ...]
    location: Path
    indexes: None | tuple[Index, ...] = Field(default=None)
