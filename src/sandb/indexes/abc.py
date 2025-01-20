from abc import ABC, abstractmethod
from typing import Any

from sandb.config import VALID_DTYPE


class Index(ABC):
    @abstractmethod
    def read(self, key: VALID_DTYPE) -> str | None:
        ...

    @abstractmethod
    def write(self, key: VALID_DTYPE, value: Any) -> None:
        ...
