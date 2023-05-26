"""Contains function for creating assignments."""
from __future__ import annotations

from typing import Iterable, Mapping, Optional

from dj_link.entities.state import Components, Identifier


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
    return {
        component: {Identifier(identifier) for identifier in identifiers}
        for component, identifiers in assignments.items()
    }
