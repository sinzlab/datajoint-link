"""Contains the gateway interface."""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable

from link.domain import events
from link.domain.link import Link


class LinkGateway(ABC):
    """Responsible for interacting with a link's persistent data."""

    @abstractmethod
    def create_link(self) -> Link:
        """Create a link from the persistent data."""

    @abstractmethod
    def apply(self, updates: Iterable[events.Update]) -> None:
        """Apply updates to the link's persistent data."""
