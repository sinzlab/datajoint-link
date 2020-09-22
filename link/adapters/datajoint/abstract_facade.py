from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Dict

from ...types import PrimaryKey

if TYPE_CHECKING:
    from .gateway import EntityDTO


class AbstractTableFacade(ABC):
    @property
    @abstractmethod
    def primary_keys(self) -> List[PrimaryKey]:
        """Returns all primary keys present in the table."""

    @abstractmethod
    def get_primary_keys_in_restriction(self, restriction) -> List[PrimaryKey]:
        """Gets all primary keys present in the table after the provided restriction is applied to it."""

    @abstractmethod
    def get_flags(self, primary_key: PrimaryKey) -> Dict[str, bool]:
        """Gets the names and values of all flags associated with the entity identified by the primary key."""

    @abstractmethod
    def fetch(self, primary_key: PrimaryKey) -> EntityDTO:
        """Fetches the entity identified by the provided primary key from the table."""

    @abstractmethod
    def insert(self, entity_dto: EntityDTO) -> None:
        """Inserts the provided entity into the table."""

    @abstractmethod
    def delete(self, primary_key: PrimaryKey) -> None:
        """Deletes the entity identified by the provided primary key from the table."""

    @abstractmethod
    def enable_flag(self, primary_key: PrimaryKey, flag_table: str) -> None:
        """Inserts the provided primary key into the flag table identified by the provided name."""

    @abstractmethod
    def disable_flag(self, primary_key: PrimaryKey, flag_table: str) -> None:
        """Deletes the provided primary key from the flag table identified by the provided name."""

    @abstractmethod
    def start_transaction(self) -> None:
        """Starts a transaction."""

    @abstractmethod
    def commit_transaction(self) -> None:
        """Commits a transaction."""

    @abstractmethod
    def cancel_transaction(self) -> None:
        """Cancels a transaction."""
