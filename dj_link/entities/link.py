"""Contains the link class."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, FrozenSet, Iterable, Mapping, Optional, TypeVar

from .custom_types import Identifier
from .state import (
    STATE_MAP,
    Components,
    Entity,
    EntityOperationResult,
    InvalidOperation,
    PersistentState,
    Processes,
    Update,
    states,
)


def create_link(
    assignments: Mapping[Components, Iterable[Identifier]],
    *,
    tainted_identifiers: Optional[Iterable[Identifier]] = None,
    processes: Optional[Mapping[Processes, Iterable[Identifier]]] = None,
) -> Link:
    """Create a new link instance."""

    def pairwise_disjoint(sets: Iterable[Iterable[Any]]) -> bool:
        union = set().union(*sets)
        return len(union) == sum(len(set(s)) for s in sets)

    T = TypeVar("T")
    V = TypeVar("V")

    def invert_mapping(mapping: Mapping[T, Iterable[V]]) -> dict[V, T]:
        return {z: x for x, y in mapping.items() for z in y}

    def validate_arguments(
        assignments: Mapping[Components, Iterable[Identifier]],
        tainted: Iterable[Identifier],
        processes: Mapping[Processes, Iterable[Identifier]],
    ) -> None:
        assert set(assignments[Components.OUTBOUND]) <= set(
            assignments[Components.SOURCE]
        ), "Outbound must not be superset of source."
        assert set(assignments[Components.LOCAL]) <= set(
            assignments[Components.OUTBOUND]
        ), "Local must not be superset of source."
        assert set(tainted) <= set(assignments[Components.SOURCE])
        assert pairwise_disjoint(processes.values()), "Identifiers can not undergo more than one process."

    def is_tainted(identifier: Identifier) -> bool:
        assert tainted_identifiers is not None
        return identifier in tainted_identifiers

    def create_entities(
        assignments: Mapping[Components, Iterable[Identifier]],
    ) -> set[Entity]:
        def create_entity(identifier: Identifier) -> Entity:
            presence = frozenset(
                component for component, identifiers in assignments.items() if identifier in identifiers
            )
            persistent_state = PersistentState(
                presence, is_tainted=is_tainted(identifier), has_process=identifier in processes_map
            )
            state = STATE_MAP[persistent_state]
            return Entity(
                identifier,
                state=state,
                current_process=processes_map.get(identifier),
                is_tainted=is_tainted(identifier),
            )

        return {create_entity(identifier) for identifier in assignments[Components.SOURCE]}

    def assign_entities(entities: Iterable[Entity]) -> dict[Components, set[Entity]]:
        def assign_to_component(component: Components) -> set[Entity]:
            return {entity for entity in entities if entity.identifier in assignments[component]}

        return {component: assign_to_component(component) for component in Components}

    if tainted_identifiers is None:
        tainted_identifiers = set()
    if processes is None:
        processes = {}
    validate_arguments(assignments, tainted_identifiers, processes)
    processes_map = invert_mapping(processes)
    entity_assignments = assign_entities(create_entities(assignments))
    return Link(
        source=Component(entity_assignments[Components.SOURCE]),
        outbound=Component(entity_assignments[Components.OUTBOUND]),
        local=Component(entity_assignments[Components.LOCAL]),
    )


@dataclass(frozen=True)
class Link:
    """The state of a link between two databases."""

    source: Component
    outbound: Component
    local: Component

    def __getitem__(self, component: Components) -> Component:
        """Return the entities in the given component."""
        component_map = {
            Components.SOURCE: self.source,
            Components.OUTBOUND: self.outbound,
            Components.LOCAL: self.local,
        }
        return component_map[component]


class Component(FrozenSet[Entity]):
    """Contains all entities present in a component."""

    @property
    def identifiers(self) -> frozenset[Identifier]:
        """Return all identifiers of entities in the component."""
        return frozenset(entity.identifier for entity in self)


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


def pull_legacy(
    link: Link,
    *,
    requested: Iterable[Identifier],
) -> set[Transfer]:
    """Create the transfer specifications needed for pulling the requested identifiers."""
    assert set(requested) <= link[Components.SOURCE].identifiers, "Requested must not be superset of source."
    assert all(
        entity.state is states.Idle for entity in link[Components.SOURCE] if entity.identifier in set(requested)
    ), "Requested entities must be idle."
    outbound_destined = set(requested) - link[Components.OUTBOUND].identifiers
    local_destined = set(requested) - link[Components.LOCAL].identifiers
    outbound_transfers = {
        Transfer(i, origin=Components.SOURCE, destination=Components.OUTBOUND, identifier_only=True)
        for i in outbound_destined
    }
    local_transfers = {
        Transfer(i, origin=Components.SOURCE, destination=Components.LOCAL, identifier_only=False)
        for i in local_destined
    }
    return outbound_transfers | local_transfers


@dataclass(frozen=True)
class LinkOperationResult:
    """Represents the result of an operation on all entities of a link."""

    updates: frozenset[Update]
    errors: frozenset[InvalidOperation]


def create_link_operation_result(results: Iterable[EntityOperationResult]) -> LinkOperationResult:
    """Create the result of an operation on a link from results of individual entities."""
    return LinkOperationResult(
        updates=frozenset(result for result in results if isinstance(result, Update)),
        errors=frozenset(result for result in results if isinstance(result, InvalidOperation)),
    )


def process(link: Link) -> LinkOperationResult:
    """Process all entities in the link producing appropriate updates."""
    return create_link_operation_result(entity.process() for entity in link[Components.SOURCE])


def _validate_requested(link: Link, requested: Iterable[Identifier]) -> None:
    assert requested, "No identifiers requested."
    assert set(requested) <= link[Components.SOURCE].identifiers, "Requested identifiers not present in link."


def pull(link: Link, *, requested: Iterable[Identifier]) -> LinkOperationResult:
    """Pull all requested entities producing appropriate updates."""
    _validate_requested(link, requested)
    return create_link_operation_result(
        entity.pull() for entity in link[Components.SOURCE] if entity.identifier in requested
    )


def delete(link: Link, *, requested: Iterable[Identifier]) -> LinkOperationResult:
    """Delete all requested identifiers producing appropriate updates."""
    _validate_requested(link, requested)
    return create_link_operation_result(
        entity.delete() for entity in link[Components.SOURCE] if entity.identifier in requested
    )
