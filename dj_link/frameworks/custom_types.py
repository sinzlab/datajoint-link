"""Contains custom types used by the infrastructure layer."""
from __future__ import annotations

from typing import Protocol, TypeVar

from dj_link.adapters.custom_types import PrimaryKey


class Table(Protocol):
    """DataJoint table protocol."""

    def fetch(self, as_dict: bool | None = ...) -> list[PrimaryKey]:
        """Fetch entities from the table."""

    def proj(self) -> Table:
        """Project the table onto its primary attributes."""

    def __and__(self: _T, condition: str | Table) -> _T:
        """Restrict the table according to the given condition."""


_T = TypeVar("_T", bound=Table)
