from functools import cached_property
from pathlib import Path
from typing import Literal, Mapping, Type, Union, get_args

from pydantic import BaseModel, ConfigDict, Field, field_serializer

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
        """
        When sending a python dtype to json convert first to
        integer value defined in VALID_DTYPE_MAPPING.

        Args:
            dtype (VALID_DTYPE): Will be a valid python dtype.

        Returns:
            VALID_DTYPE_ALIAS: The integer alias for the supplied
                               dtype defined by VALID_DTYPE_MAPPING.
        """
        return VALID_DTYPE_MAPPING[dtype]


class TableMetadata(BaseModel):
    """
    Holds metadata about a given table.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    columns: tuple[Column, ...]
    location: Path
    indexes: None | tuple[Index, ...] = Field(default=None)

    @cached_property
    def dtypes(self) -> tuple[Type[VALID_DTYPE], ...]:
        return tuple(column.dtype for column in self.columns)

    @cached_property
    def col_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    def metadata_path(self) -> Path:
        return self.location / self.name / "metadata.json"

    def data_path(self) -> Path:
        return self.location / self.name / "data.csv"
