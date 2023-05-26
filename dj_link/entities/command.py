"""Contains all commands for the persistence layer."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .state import Identifier


@dataclass(frozen=True)
class Command:
    """A command to be executed by the persistence layer and produced by an entity undergoing a state transition."""

    identifier: Identifier


@dataclass(frozen=True)
class AddToOutbound(Command):
    """A command to add an entity to the outbound component."""


@dataclass(frozen=True)
class RemoveFromOutbound(Command):
    """A command to remove an entity from the outbound component."""


@dataclass(frozen=True)
class AddToLocal(Command):
    """A command to add an entity to the outbound component."""


@dataclass(frozen=True)
class RemoveFromLocal(Command):
    """A command to remove an entity from the outbound component."""


@dataclass(frozen=True)
class StartPullOperation(Command):
    """A command starting the pull operation on an entity."""


@dataclass(frozen=True)
class FinishPullOperation(Command):
    """A command finishing the pull operation on an entity."""


@dataclass(frozen=True)
class StartDeleteOperation(Command):
    """A command starting the delete operation on an entity."""


@dataclass(frozen=True)
class FinishDeleteOperation(Command):
    """A command finishing the delete operation on an entity."""


@dataclass(frozen=True)
class Flag(Command):
    """A command flagging an entity for deletion."""


@dataclass(frozen=True)
class Unflag(Command):
    """A command unflagging an entity for deletion."""
