"""Specifies the interface of the DataJoint table facade as expected by the adapters."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict, List

from ...custom_types import PrimaryKey

if TYPE_CHECKING:
    from .gateway import EntityDTO


class AbstractTableFacade(ABC):
    """Specifies the interface of the DataJoint table facade as expected by the adapters."""

    @property
    @abstractmethod
    def primary_keys(self) -> List[PrimaryKey]:
        """Return all primary keys present in the table."""

    @abstractmethod
    def get_primary_keys_in_restriction(self, restriction) -> List[PrimaryKey]:
        """Get all primary keys present in the table after the provided restriction is applied to it."""

    @abstractmethod
    def get_flags(self, primary_key: PrimaryKey) -> Dict[str, bool]:
        """Get the names and values of all flags associated with the entity identified by the primary key."""

    @abstractmethod
    def fetch(self, primary_key: PrimaryKey) -> EntityDTO:
        """Fetch the entity identified by the provided primary key from the table."""

    @abstractmethod
    def insert(self, entity_dto: EntityDTO) -> None:
        """Insert the provided entity into the table."""

    @abstractmethod
    def delete(self, primary_key: PrimaryKey) -> None:
        """Delete the entity identified by the provided primary key from the table."""

    @abstractmethod
    def enable_flag(self, primary_key: PrimaryKey, flag_table: str) -> None:
        """Insert the provided primary key into the flag table identified by the provided name."""

    @abstractmethod
    def disable_flag(self, primary_key: PrimaryKey, flag_table: str) -> None:
        """Delete the provided primary key from the flag table identified by the provided name."""

    @abstractmethod
    def start_transaction(self) -> None:
        """Start a transaction."""

    @abstractmethod
    def commit_transaction(self) -> None:
        """Commit a transaction."""

    @abstractmethod
    def cancel_transaction(self) -> None:
        """Cancel a transaction."""
