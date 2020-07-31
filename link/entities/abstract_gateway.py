from abc import ABC, abstractmethod
from typing import Any, List, Dict, Optional


class AbstractEntityDTO(ABC):
    """Defines the interface of the data transfer object that is passed between the gateway and the use-cases."""

    @property
    @abstractmethod
    def identifier_data(self) -> Any:
        """Contains all the data used to compute the unique identifier of the entity."""

    @property
    @abstractmethod
    def non_identifier_data(self) -> Optional[Any]:
        """Contains all the data not used to compute the unique identifier of the entity."""


class AbstractGateway(ABC):
    """Defines the interface of the gateway as expected by the entities."""

    @property
    @abstractmethod
    def identifiers(self) -> List[str]:
        """Returns the identifiers of all the entities in the gateway."""

    @abstractmethod
    def get_flags(self, identifier: str) -> Dict[str, bool]:
        """Gets the flags associated with the entity specified by the provided identifier."""

    @abstractmethod
    def fetch(self, identifier: str) -> AbstractEntityDTO:
        """Fetches an entity."""

    @abstractmethod
    def insert(self, data: AbstractEntityDTO) -> None:
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
