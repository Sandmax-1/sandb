from abc import ABC, abstractmethod
from typing import Any, Protocol


class Comparable(Protocol):
    """Protocol for annotating comparable types."""

    def __lt__(self, other: Any, /) -> bool:
        ...


class Index(ABC):
    @abstractmethod
    def read(self, key: Comparable) -> str | None:
        ...

    @abstractmethod
    def write(self, key: Comparable, value: Any) -> None:
        ...
