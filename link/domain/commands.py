"""Contains all domain commands."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Command:
    """Base class for all commands."""


@dataclass(frozen=True)
class ListIdleEntities(Command):
    """Start the delete process for the requested entities."""
