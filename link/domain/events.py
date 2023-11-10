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
class UnsharedEntitiesListed(Event):
    """Unshared entities in a link have been listed."""

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
class BatchProcessingStarted(Event):
    """The processing of a batch of entities started."""

    process: Processes
    identifiers: frozenset[Identifier]


@dataclass(frozen=True)
class BatchProcessingFinished(Event):
    """The processing of a batch of entities finished."""

    process: Processes
    identifiers: frozenset[Identifier]
