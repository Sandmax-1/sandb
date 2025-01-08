from pathlib import Path
from typing import Literal, Mapping, Union, get_args

CURRENT_DIR = Path(__file__).absolute()
ROOT_FOLDER = "sandb"
ROOT_DIR = next(p for p in CURRENT_DIR.parents if p.parts[-1] == ROOT_FOLDER)


VALID_DTYPE = Union[str, int]
VALID_DTYPE_ALIAS = Literal[0, 1]

VALID_DTYPE_MAPPING: Mapping[VALID_DTYPE, VALID_DTYPE_ALIAS] = dict(
    zip(get_args(VALID_DTYPE), get_args(VALID_DTYPE_ALIAS))
)
REVERSE_DTYPE_MAPPING: Mapping[VALID_DTYPE_ALIAS, VALID_DTYPE] = {
    v: k for k, v in VALID_DTYPE_MAPPING.items()
}
