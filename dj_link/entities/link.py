"""Contains the link class."""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Mapping, NewType


class Components(Enum):
    """Names for the different components in a link."""

    SOURCE = 1
    OUTBOUND = 2
    LOCAL = 3


Identifier = NewType("Identifier", str)


def create_link(assignments: Mapping[Components, Iterable[Identifier]]) -> Link:
    """Create a new link instance."""
    return Link(
        source=set(assignments[Components.SOURCE]),
        outbound=set(assignments[Components.OUTBOUND]),
        local=set(assignments[Components.LOCAL]),
    )


@dataclass(frozen=True)
class Link:
    """The state of a link between two databases."""

    source: set[Identifier]
    outbound: set[Identifier]
    local: set[Identifier]

    def __post_init__(self) -> None:
        """Validate the created link."""
        assert self.outbound <= self.source, "Outbound must not be superset of source."
        assert self.local == self.outbound, "Local and outbound must be identical."


@dataclass(frozen=True)
class Transfer:
    """Specification for the transfer of an identifier from one component to another within a link."""

    identifier: Identifier
    origin: Components
    destination: Components
    identifier_only: bool

    def __post_init__(self) -> None:
        """Validate the created specification."""
        assert self.origin is Components.SOURCE, "Origin must be source."
        assert self.destination in (Components.OUTBOUND, Components.LOCAL), "Destiny must be outbound or local."
        if self.destination is Components.OUTBOUND:
            assert self.identifier_only, "Only identifier can be transferred to outbound."
        else:
            assert not self.identifier_only, "Whole entity must be transferred to local."


def pull(
    link: Link,
    *,
    requested: Iterable[Identifier],
) -> set[Transfer]:
    """Create the transfer specifications needed for pulling the requested identifiers."""
    assert set(requested) <= link.source, "Requested must not be superset of source."
    outbound_destined = set(requested) - link.outbound
    local_destined = set(requested) - link.local
    outbound_transfers = {
        Transfer(i, origin=Components.SOURCE, destination=Components.OUTBOUND, identifier_only=True)
        for i in outbound_destined
    }
    local_transfers = {
        Transfer(i, origin=Components.SOURCE, destination=Components.LOCAL, identifier_only=False)
        for i in local_destined
    }
    return outbound_transfers | local_transfers
