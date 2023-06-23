"""Contains interface of a facade around a link that is persisted using DataJoint."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal, Union

from dj_link.custom_types import PrimaryKey


class DJLinkFacade(ABC):
    """A facade around a link that is persisted using DataJoint."""

    @abstractmethod
    def get_source_primary_keys(self) -> list[PrimaryKey]:
        """Get all primary keys present in the source component."""

    @abstractmethod
    def get_outbound_primary_keys(self) -> list[PrimaryKey]:
        """Get all primary keys present in the outbound component."""

    @abstractmethod
    def get_local_primary_keys(self) -> list[PrimaryKey]:
        """Get all primary keys present in the local component."""

    @abstractmethod
    def get_tainted_primary_keys(self) -> list[PrimaryKey]:
        """Get all tainted primary keys."""

    @abstractmethod
    def get_processes(self) -> list[DJProcess]:
        """Get all processes associated with entities."""

    @abstractmethod
    def add_to_local(self, primary_key: PrimaryKey) -> None:
        """Add the entity identified by the given primary key to the local component."""

    @abstractmethod
    def remove_from_local(self, primary_key: PrimaryKey) -> None:
        """Remove the entity identified by the given primary key from the local component."""

    @abstractmethod
    def start_pull_process(self, primary_key: PrimaryKey) -> None:
        """Start the pull process for the entity identified by the given primary key."""

    @abstractmethod
    def finish_pull_process(self, primary_key: PrimaryKey) -> None:
        """Finish the pull process for the entity identified by the given primary key."""

    @abstractmethod
    def start_delete_process(self, primary_key: PrimaryKey) -> None:
        """Start the delete process for the entity identified by the given primary key."""

    @abstractmethod
    def finish_delete_process(self, primary_key: PrimaryKey) -> None:
        """Finish the delete process for the entity identified by the given primary key."""

    @abstractmethod
    def deprecate(self, primary_key: PrimaryKey) -> None:
        """Deprecate the entity identified by the given primary key."""


@dataclass(frozen=True)
class DJProcess:
    """An association between an entity's primary key and its process."""

    primary_key: PrimaryKey
    current_process: Union[Literal["PULL"], Literal["DELETE"], Literal["NONE"]]