from abc import ABC, abstractmethod
from typing import Protocol, TypeVar

Comparable = TypeVar("Comparable", bound="ComparableType")


class ComparableType(Protocol):
    """Protocol for annotating comparable types."""

    def __lt__(self: Comparable, other: Comparable) -> bool:
        pass


class Index(ABC):
    @abstractmethod
    def read(self, key: Comparable) -> str | None:
        ...

    @abstractmethod
    def write(self, key: Comparable, data: str) -> None:
        ...
