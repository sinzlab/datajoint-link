"""Contains interfaces for relaying information about the progress of processing a batch of entities."""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable

from link.domain.custom_types import Identifier
from link.domain.state import Processes


class ProgessDisplay(ABC):
    """Shows information about the progress of a batch of entities being processed to the user."""

    @abstractmethod
    def start(self, process: Processes, to_be_processed: Iterable[Identifier]) -> None:
        """Start showing progress information to the user."""

    @abstractmethod
    def update_current(self, new: Identifier) -> None:
        """Update the display to reflect a new entity being currently processed."""

    @abstractmethod
    def finish_current(self) -> None:
        """Update the display to reflect that the current entity finished processing."""

    @abstractmethod
    def stop(self) -> None:
        """Stop showing progress information to the user."""
