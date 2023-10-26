"""Contains all domain commands."""
from __future__ import annotations

from dataclasses import dataclass

from .custom_types import Identifier


@dataclass(frozen=True)
class Command:
    """Base class for all commands."""


@dataclass(frozen=True)
class PullEntities(Command):
    """Pull the requested entities."""

    requested: frozenset[Identifier]


@dataclass(frozen=True)
class DeleteEntities(Command):
    """Delete the requested entities."""

    requested: frozenset[Identifier]


@dataclass(frozen=True)
class ListIdleEntities(Command):
    """Start the delete process for the requested entities."""
