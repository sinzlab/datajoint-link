"""Contains all domain events."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .custom_types import Identifier

if TYPE_CHECKING:
    from .state import Commands, Operations, Processes, State, Transition


@dataclass(frozen=True)
class Event:
    """Base class for all events."""


@dataclass(frozen=True)
class OperationApplied(Event):
    """An operation was applied to an entity."""

    operation: Operations
    identifier: Identifier


@dataclass(frozen=True)
class InvalidOperationRequested(OperationApplied):
    """An operation that is invalid given the entities current state was requested."""

    state: type[State]


@dataclass(frozen=True)
class StateChanged(OperationApplied):
    """The state of an entity changed during the application of an operation."""

    transition: Transition
    command: Commands


@dataclass(frozen=True)
class IdleEntitiesListed(Event):
    """Idle entities in a link have been listed."""

    identifiers: frozenset[Identifier]


@dataclass(frozen=True)
class ProcessStarted(Event):
    """A process for an entity was started."""

    process: Processes
    identifier: Identifier


@dataclass(frozen=True)
class ProcessFinished(Event):
    """A process for an entity was finished."""

    process: Processes
    identifier: Identifier


@dataclass(frozen=True)
class ProcessesStarted(Event):
    """The same process has been started for multiple entities."""

    process: Processes
    identifiers: frozenset[Identifier]


@dataclass(frozen=True)
class ProcessesFinished(Event):
    """The same process has been finished for multiple entities."""

    process: Processes
    identifiers: frozenset[Identifier]
