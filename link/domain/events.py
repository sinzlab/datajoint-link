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

    operation: Operations
    identifier: Identifier


@dataclass(frozen=True)
class InvalidOperationRequested(Event):
    """An operation that is invalid given the entities current state was requested."""

    state: type[State]


@dataclass(frozen=True)
class EntityStateChanged(Event):
    """The state of an entity changed during the application of an operation."""

    transition: Transition
    command: Commands
