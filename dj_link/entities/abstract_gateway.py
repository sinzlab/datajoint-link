"""Specifies the interface of the gateway as expected by the entities."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Generic, Iterator, List, TypeVar


class AbstractEntityDTO(ABC):  # pylint: disable=too-few-public-methods
    """Defines the interface of the data transfer object containing an entity's data."""

    @abstractmethod
    def create_identifier_only_copy(self) -> AbstractEntityDTO:
        """Create a copy of the instance containing only the data used to compute the unique identifier."""


EntityDTO = TypeVar("EntityDTO", bound=AbstractEntityDTO)


class AbstractGateway(ABC, Generic[EntityDTO]):
    """Define the interface of the gateway as expected by the entities."""

    @property
    @abstractmethod
    def identifiers(self) -> List[str]:
        """Return the identifiers of all the entities in the gateway."""

    @abstractmethod
    def get_flags(self, identifier: str) -> Dict[str, bool]:
        """Get the flags associated with the entity specified by the provided identifier."""

    @abstractmethod
    def fetch(self, identifier: str) -> EntityDTO:
        """Fetch an entity.

        Raise KeyError if the entity is missing.
        """

    @abstractmethod
    def insert(self, entity_dto: EntityDTO) -> None:
        """Insert an entity."""

    @abstractmethod
    def delete(self, identifier: str) -> None:
        """Delete an entity."""

    @abstractmethod
    def set_flag(self, identifier: str, flag: str, value: bool) -> None:
        """Set the flag of the entity specified by the provided identifier to the provided value."""

    @abstractmethod
    def start_transaction(self) -> None:
        """Start a transaction."""

    @abstractmethod
    def commit_transaction(self) -> None:
        """Commit a transaction."""

    @abstractmethod
    def cancel_transaction(self) -> None:
        """Cancel a transaction."""

    @abstractmethod
    def __len__(self) -> int:
        """Return the number of entities in the gateway."""

    @abstractmethod
    def __iter__(self) -> Iterator[str]:
        """Iterate over all identifiers in the table."""
