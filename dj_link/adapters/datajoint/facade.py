"""Contains interface of a facade around a link that is persisted using DataJoint."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal, Union

from dj_link.adapters.datajoint import PrimaryKey


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


@dataclass(frozen=True)
class DJProcess:
    """An association between an entity's primary key and its process."""

    primary_key: PrimaryKey
    current_process: Union[Literal["PULL"], Literal["DELETE"], Literal["NONE"]]


@dataclass(frozen=True)
class DJAssignments:
    """The assignments of primary keys to the different components."""

    source: list[PrimaryKey]
    outbound: list[PrimaryKey]
    local: list[PrimaryKey]
