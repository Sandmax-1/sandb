from pathlib import Path
from typing import Any, Literal, Mapping, Type, Union, get_args

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from sandb.indexes.abc import Index

VALID_DTYPE_ALIAS = Literal[0, 1]
VALID_DTYPE = Union[str, int]

VALID_DTYPE_MAPPING: Mapping[VALID_DTYPE, VALID_DTYPE_ALIAS] = dict(
    zip(get_args(VALID_DTYPE), get_args(VALID_DTYPE_ALIAS))
)

REVERSE_DTYPE_MAPPING: Mapping[VALID_DTYPE_ALIAS, VALID_DTYPE] = {
    v: k for k, v in VALID_DTYPE_MAPPING.items()
}


class Column(BaseModel):
    """
    Holds the metadata about a column. Only needs column name and dtype.
    dtype only allowed to come from VALID_DTYPES list.
    """

    name: str
    dtype: Type[VALID_DTYPE]

    @field_serializer("dtype")
    def serialize_dtype(self, dtype: VALID_DTYPE) -> VALID_DTYPE_ALIAS:
        return VALID_DTYPE_MAPPING[dtype]

    @field_validator("dtype", mode="before")
    def deserialize_dtype(cls, value: Any) -> VALID_DTYPE:
        if value in REVERSE_DTYPE_MAPPING.keys():
            return REVERSE_DTYPE_MAPPING[value]
        # This will get cleaned up by pydantic so ignore mypy
        return value  # type: ignore[no-any-return]


class TableMetadata(BaseModel):
    """
    Holds metadata about a given table.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    columns: tuple[Column, ...]
    location: Path
    indexes: None | tuple[Index, ...] = Field(default=None)
