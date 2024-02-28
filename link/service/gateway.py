"""Contains the gateway interface."""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable

from link.domain import events
from link.domain.custom_types import Identifier
from link.domain.state import Entity


class LinkGateway(ABC):
    """Responsible for interacting with a link's persistent data."""

    @abstractmethod
    def create_entity(self, identifier: Identifier) -> Entity:
        """Create a entity instance from persistent data."""

    @abstractmethod
    def apply(self, updates: Iterable[events.StateChanged]) -> None:
        """Apply updates to the link's persistent data."""
