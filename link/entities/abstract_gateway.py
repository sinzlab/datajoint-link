from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List, Dict, TypeVar, Generic


class AbstractEntityDTO(ABC):
    """Defines the interface of the data transfer object containing an entity's data."""

    @abstractmethod
    def create_identifier_only_copy(self) -> AbstractEntityDTO:
        """Creates a copy of the instance containing only the data used to compute the unique identifier."""


EntityDTO = TypeVar("EntityDTO", bound=AbstractEntityDTO)


class AbstractGateway(ABC, Generic[EntityDTO]):
    """Defines the interface of the gateway as expected by the entities."""

    @property
    @abstractmethod
    def identifiers(self) -> List[str]:
        """Returns the identifiers of all the entities in the gateway."""

    @abstractmethod
    def get_flags(self, identifier: str) -> Dict[str, bool]:
        """Gets the flags associated with the entity specified by the provided identifier."""

    @abstractmethod
    def fetch(self, identifier: str) -> EntityDTO:
        """Fetches an entity."""

    @abstractmethod
    def insert(self, entity_dto: EntityDTO) -> None:
        """Inserts an entity."""

    @abstractmethod
    def delete(self, identifier: str) -> None:
        """Deletes an entity."""

    @abstractmethod
    def set_flag(self, identifier: str, flag: str, value: bool) -> None:
        """Sets the flag of the entity specified by the provided identifier to the provided value."""

    @abstractmethod
    def start_transaction(self) -> None:
        """Starts a transaction."""

    @abstractmethod
    def commit_transaction(self) -> None:
        """Commits a transaction."""

    @abstractmethod
    def cancel_transaction(self) -> None:
        """Cancels a transaction."""
