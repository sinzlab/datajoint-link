"""Contains views for showing progress information to the user."""
from __future__ import annotations

import logging
from typing import NoReturn

from tqdm.auto import tqdm

from link.adapters.progress import ProgressView

logger = logging.getLogger(__name__)


class TQDMProgressView(ProgressView):
    """A view that uses tqdm to show a progress bar."""

    def __init__(self) -> None:
        """Initialize the view."""
        self.__progress_bar: tqdm[NoReturn] | None = None
        self._is_disabled: bool = False

    @property
    def _progress_bar(self) -> tqdm[NoReturn]:
        assert self.__progress_bar
        return self.__progress_bar

    def open(self, description: str, total: int, unit: str) -> None:
        """Start showing the progress bar."""
        self.__progress_bar = tqdm(total=total, desc=description, unit=unit, disable=self._is_disabled)

    def update_current(self, new: str) -> None:
        """Update information about the current iteration shown at the end of the bar."""
        self._progress_bar.set_postfix(current=new)

    def update_iteration(self) -> None:
        """Update the bar to show an iteration finished."""
        self._progress_bar.update()

    def close(self) -> None:
        """Stop showing the progress bar."""
        self._progress_bar.close()
        self.__progress_bar = None

    def enable(self) -> None:
        """Enable the progress bar."""
        self._is_disabled = False

    def disable(self) -> None:
        """Disable the progress bar."""
        self._is_disabled = True
