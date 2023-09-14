"""Contains function for creating assignments."""
from __future__ import annotations

from typing import Iterable, Mapping, Optional
from uuid import UUID, uuid4

from dj_link.domain.custom_types import Identifier
from dj_link.domain.state import Components

__UUIDS: dict[str, UUID] = {}


def create_identifier(name: str) -> Identifier:
    return Identifier(__UUIDS.setdefault(name, uuid4()))


def create_identifiers(*names: str) -> set[Identifier]:
    return {create_identifier(name) for name in names}


def create_assignments(
    assignments: Optional[Mapping[Components, Iterable[str]]] = None
) -> dict[Components, set[Identifier]]:
    """Create assignments of identifiers to components."""
    if assignments is None:
        assignments = {}
    else:
        assignments = dict(assignments)
    for component in Components:
        if component not in assignments:
            assignments[component] = set()
    return {component: create_identifiers(*identifiers) for component, identifiers in assignments.items()}
