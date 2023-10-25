"""Contains all domain commands."""
from __future__ import annotations

from dataclasses import dataclass

from .custom_types import Identifier


@dataclass(frozen=True)
class Command:
    """Base class for all commands."""

    requested: frozenset[Identifier]


@dataclass(frozen=True)
class ProcessLink(Command):
    """Process the requested entities in the link."""


@dataclass(frozen=True)
class StartPullProcess(Command):
    """Start the pull process for the requested entities."""


@dataclass(frozen=True)
class StartDeleteProcess(Command):
    """Start the delete process for the requested entities."""
