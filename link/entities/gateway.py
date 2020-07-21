from abc import ABC, abstractmethod
from typing import Any, List, Dict


class AbstractGateway(ABC):
    """A abstract base class defining the interface for the gateway as expected by the entities."""

    @property
    @abstractmethod
    def identifiers(self) -> List[str]:
        """Returns the identifiers of all the entities in the gateway."""

    @abstractmethod
    def get_flags(self, identifier: str) -> Dict[str, bool]:
        """Gets the flags associated with the entity specified by the provided identifier."""

    @abstractmethod
    def fetch(self, identifier: str) -> Any:
        """Fetches an entity."""

    @abstractmethod
    def insert(self, identifier: str, data: Any) -> None:
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
