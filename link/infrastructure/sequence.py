"""Contains sequence related functionality."""
from __future__ import annotations

import collections
from collections.abc import MutableSequence
from typing import TYPE_CHECKING, Callable, Iterable, Iterator, TypeVar

if TYPE_CHECKING:
    UserList = collections.UserList
else:

    class _UserList:
        def __getitem__(*args):
            return collections.UserList

    UserList = _UserList()
_T = TypeVar("_T")


class IterationCallbackList(UserList[_T]):
    """A list that invokes a callback whenever it gets iterated over."""

    def __init__(self, data: Iterable[_T] | None = None) -> None:
        """Initialize the list."""
        super().__init__(data)
        self.callback: Callable[[], None] | None = None

    def __iter__(self) -> Iterator[_T]:
        """Iterate over the list."""
        if self.callback is not None:
            self.callback()
        return super().__iter__()


_V = TypeVar("_V")


def create_content_replacer(sequence: MutableSequence[_V]) -> Callable[[Iterable[_V]], None]:
    """Create a callable that replaces the sequence's contents in-place when called."""

    def replace_contents(new: Iterable[_V]) -> None:
        sequence.clear()
        sequence.extend(new)

    return replace_contents
