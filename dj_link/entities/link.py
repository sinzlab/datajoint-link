"""Contains the link class."""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from functools import reduce
from typing import Mapping, NewType


class Components(Enum):
    """Names for the different components in a link."""

    SOURCE = 1
    OUTBOUND = 2
    LOCAL = 3


class States(Enum):
    """Names for the different states of an entity."""

    IDLE = 1
    PULLED = 2


Identifier = NewType("Identifier", str)


@dataclass(frozen=True)
class Entity:
    """An entity in a link."""

    identifier: Identifier
    state: States


def create_link(assignments: Mapping[Components, Iterable[Identifier]]) -> Link:
    """Create a new link instance."""

    def validate_assignments(assignments: Mapping[Components, Iterable[Identifier]]) -> None:
        assert set(assignments[Components.OUTBOUND]) <= set(
            assignments[Components.SOURCE]
        ), "Outbound must not be superset of source."
        assert set(assignments[Components.LOCAL]) == set(
            assignments[Components.OUTBOUND]
        ), "Local and outbound must be identical."

    def create_entities(assignments: Mapping[Components, Iterable[Identifier]]) -> set[Entity]:
        def create_identifier_union(assignments: Mapping[Components, Iterable[Identifier]]) -> Iterable[Identifier]:
            return reduce(lambda x, y: set(x) | set(y), assignments.values())

        def create_entity(identifier: Identifier) -> Entity:
            presence = frozenset(
                component for component, identifiers in assignments.items() if identifier in identifiers
            )
            return Entity(identifier, state=presence_map[presence])

        presence_map = {
            frozenset({Components.SOURCE}): States.IDLE,
            frozenset({Components.SOURCE, Components.OUTBOUND, Components.LOCAL}): States.PULLED,
        }
        return {create_entity(identifier) for identifier in create_identifier_union(assignments)}

    def assign_entities(entities: Iterable[Entity]) -> dict[Components, set[Entity]]:
        def assign_to_component(component: Components) -> set[Entity]:
            return {entity for entity in entities if entity.identifier in assignments[component]}

        return {component: assign_to_component(component) for component in Components}

    validate_assignments(assignments)
    entity_assignments = assign_entities(create_entities(assignments))
    return Link(
        source=frozenset(entity_assignments[Components.SOURCE]),
        outbound=frozenset(entity_assignments[Components.OUTBOUND]),
        local=frozenset(entity_assignments[Components.LOCAL]),
    )


@dataclass(frozen=True)
class Link:
    """The state of a link between two databases."""

    source: frozenset[Entity]
    outbound: frozenset[Entity]
    local: frozenset[Entity]

    def __getitem__(self, component: Components) -> frozenset[Entity]:
        """Return the entities in the given component."""
        component_map = {
            Components.SOURCE: self.source,
            Components.OUTBOUND: self.outbound,
            Components.LOCAL: self.local,
        }
        return component_map[component]


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
    assert set(requested) <= {entity.identifier for entity in link.source}, "Requested must not be superset of source."
    assert all(
        entity.state is States.IDLE for entity in link.source if entity.identifier in set(requested)
    ), "Requested entities must be idle."
    outbound_destined = set(requested) - {entity.identifier for entity in link.outbound}
    local_destined = set(requested) - {entity.identifier for entity in link.local}
    outbound_transfers = {
        Transfer(i, origin=Components.SOURCE, destination=Components.OUTBOUND, identifier_only=True)
        for i in outbound_destined
    }
    local_transfers = {
        Transfer(i, origin=Components.SOURCE, destination=Components.LOCAL, identifier_only=False)
        for i in local_destined
    }
    return outbound_transfers | local_transfers
