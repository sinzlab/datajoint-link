"""Contains all domain commands."""
from __future__ import annotations

from dataclasses import dataclass

from .custom_types import Identifier


@dataclass(frozen=True)
class Command:
    """Base class for all commands."""


@dataclass(frozen=True)
class BatchCommand(Command):
    """Base class for all commands dealing with a batch of entities."""

    requested: frozenset[Identifier]


@dataclass(frozen=True)
class PullEntity(Command):
    """Pull the requested entity."""

    requested: Identifier


@dataclass(frozen=True)
class DeleteEntity(Command):
    """Delete the requested entity."""

    requested: Identifier


@dataclass(frozen=True)
class PullEntities(BatchCommand):
    """Pull the requested entities."""


@dataclass(frozen=True)
class DeleteEntities(BatchCommand):
    """Delete the requested entities."""
