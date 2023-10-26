"""Contains all domain events."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .custom_types import Identifier

if TYPE_CHECKING:
    from .state import Commands, Operations, State, Transition


@dataclass(frozen=True)
class Event:
    """Base class for all events."""


@dataclass(frozen=True)
class EntityOperationApplied(Event):
    """An operation was applied to an entity."""

    operation: Operations
    identifier: Identifier


@dataclass(frozen=True)
class InvalidOperationRequested(EntityOperationApplied):
    """An operation that is invalid given the entities current state was requested."""

    state: type[State]


@dataclass(frozen=True)
class EntityStateChanged(EntityOperationApplied):
    """The state of an entity changed during the application of an operation."""

    transition: Transition
    command: Commands


@dataclass(frozen=True)
class LinkStateChanged(Event):
    """The state of a link changed during the application of an operation."""

    operation: Operations
    requested: frozenset[Identifier]
    updates: frozenset[EntityStateChanged]
    errors: frozenset[InvalidOperationRequested]

    def __post_init__(self) -> None:
        """Validate the event."""
        assert all(
            result.operation is self.operation for result in (self.updates | self.errors)
        ), "Not all events have same operation."


@dataclass(frozen=True)
class IdleEntitiesListed(Event):
    """Idle entities in a link have been listed."""

    identifiers: frozenset[Identifier]


@dataclass(frozen=True)
class EntitiesPulled(Event):
    """The requested entities have been pulled."""

    requested: frozenset[Identifier]
    errors: frozenset[InvalidOperationRequested]


@dataclass(frozen=True)
class EntitiesDeleted(Event):
    """The requested entities have been deleted."""

    requested: frozenset[Identifier]
