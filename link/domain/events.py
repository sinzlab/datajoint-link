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
class InvalidOperation(Event):
    """Represents the result of attempting an operation that is invalid in the entity's current state."""

    state: type[State]


@dataclass(frozen=True)
class Update(Event):
    """Represents the persistent update needed to transition an entity."""

    transition: Transition
    command: Commands
