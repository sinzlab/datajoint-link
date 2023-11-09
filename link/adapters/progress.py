"""Contains DataJoint-specific code for relaying progress information to the user."""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable

from link.domain.custom_types import Identifier
from link.domain.state import Processes
from link.service.progress import ProgessDisplay

from .identification import IdentificationTranslator


class ProgressView(ABC):
    """Progress display."""

    @abstractmethod
    def open(self, description: str, total: int, unit: str) -> None:
        """Open the progress display showing information to the user."""

    @abstractmethod
    def update_current(self, new: str) -> None:
        """Update the display with new information regarding the current iteration."""

    @abstractmethod
    def update_iteration(self) -> None:
        """Update the display to reflect that the current iteration finished."""

    @abstractmethod
    def close(self) -> None:
        """Close the progress display."""

    @abstractmethod
    def enable(self) -> None:
        """Enable the view."""

    @abstractmethod
    def disable(self) -> None:
        """Disable the view."""


class DJProgressDisplayAdapter(ProgessDisplay):
    """DataJoint-specific adapter for the progress display."""

    def __init__(self, translator: IdentificationTranslator, display: ProgressView) -> None:
        """Initialize the display."""
        self._translator = translator
        self._display = display

    def start(self, process: Processes, to_be_processed: Iterable[Identifier]) -> None:
        """Start showing progress information to the user."""
        self._display.open(process.name, len(list(to_be_processed)), "row")

    def update_current(self, new: Identifier) -> None:
        """Update the display to reflect a new entity being currently processed."""
        self._display.update_current(repr(self._translator.to_primary_key(new)))

    def finish_current(self) -> None:
        """Update the display to reflect that the current entity finished processing."""
        self._display.update_iteration()

    def stop(self) -> None:
        """Stop showing progress information to the user."""
        self._display.close()
