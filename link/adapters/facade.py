"""Contains interface of a facade around a link that is persisted using DataJoint."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from .custom_types import PrimaryKey


class DJLinkFacade(ABC):
    """A facade around a link that is persisted using DataJoint."""

    @abstractmethod
    def get_assignments(self) -> DJAssignments:
        """Get the assignments of primary keys to the different components."""

    @abstractmethod
    def get_tainted_primary_keys(self) -> list[PrimaryKey]:
        """Get all tainted primary keys."""

    @abstractmethod
    def get_processes(self) -> list[DJProcess]:
        """Get all processes associated with entities."""

    @abstractmethod
    def get_assignment(self, primary_key: PrimaryKey) -> DJAssignment:
        """Get the assignment of the entity with the given primary key to components."""

    @abstractmethod
    def get_condition(self, primary_key: PrimaryKey) -> DJCondition:
        """Get the condition of the entity with the given primary key."""

    @abstractmethod
    def get_process(self, primary_key: PrimaryKey) -> DJProcess:
        """Get the process of the entity with the given primary key."""

    @abstractmethod
    def add_to_local(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Add the entity identified by the given primary key to the local component."""

    @abstractmethod
    def remove_from_local(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Remove the entity identified by the given primary key from the local component."""

    @abstractmethod
    def start_pull_process(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Start the pull process for the entity identified by the given primary key."""

    @abstractmethod
    def finish_pull_process(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Finish the pull process for the entity identified by the given primary key."""

    @abstractmethod
    def start_delete_process(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Start the delete process for the entity identified by the given primary key."""

    @abstractmethod
    def finish_delete_process(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Finish the delete process for the entity identified by the given primary key."""

    @abstractmethod
    def deprecate(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Deprecate the entity identified by the given primary key."""


ProcessType = Literal["PULL", "DELETE", "NONE"]


@dataclass(frozen=True)
class DJProcess:
    """An association between an entity's primary key and its process."""

    primary_key: PrimaryKey
    current_process: ProcessType


@dataclass(frozen=True)
class DJAssignments:
    """The assignments of primary keys to the different components."""

    source: list[PrimaryKey]
    outbound: list[PrimaryKey]
    local: list[PrimaryKey]


@dataclass(frozen=True)
class DJAssignment:
    """The presence of a specific entity in the three different tables that make up its link."""

    primary_key: PrimaryKey
    source: bool
    outbound: bool
    local: bool


@dataclass(frozen=True)
class DJCondition:
    """The condition of a specific entity."""

    primary_key: PrimaryKey
    is_flagged: bool
