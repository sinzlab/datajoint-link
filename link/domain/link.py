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
    Operations,
    PersistentState,
    Processes,
    Update,
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
                current_process=processes_map.get(identifier, Processes.NONE),
                is_tainted=is_tainted(identifier),
                operation_results=tuple(),
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
    return Link(entity_assignments[Components.SOURCE])


class Link(FrozenSet[Entity]):
    """The state of a link between two databases."""

    @property
    def identifiers(self) -> frozenset[Identifier]:
        """Return the identifiers of all entities in the link."""
        return frozenset(entity.identifier for entity in self)


@dataclass(frozen=True)
class LinkOperationResult:
    """Represents the result of an operation on all entities of a link."""

    operation: Operations
    updates: frozenset[Update]
    errors: frozenset[InvalidOperation]

    def __post_init__(self) -> None:
        """Validate the result."""
        assert all(
            result.operation is self.operation for result in (self.updates | self.errors)
        ), "Not all results have same operation."


def create_link_operation_result(results: Iterable[EntityOperationResult]) -> LinkOperationResult:
    """Create the result of an operation on a link from results of individual entities."""
    results = set(results)
    operation = next(iter(results)).operation
    return LinkOperationResult(
        operation,
        updates=frozenset(result for result in results if isinstance(result, Update)),
        errors=frozenset(result for result in results if isinstance(result, InvalidOperation)),
    )


def process(link: Link, *, requested: Iterable[Identifier]) -> LinkOperationResult:
    """Process all entities in the link producing appropriate updates."""
    _validate_requested(link, requested)
    return create_link_operation_result(
        entity.apply(Operations.PROCESS).operation_results[0] for entity in link if entity.identifier in requested
    )


def _validate_requested(link: Link, requested: Iterable[Identifier]) -> None:
    assert requested, "No identifiers requested."
    assert set(requested) <= link.identifiers, "Requested identifiers not present in link."


def start_pull(link: Link, *, requested: Iterable[Identifier]) -> LinkOperationResult:
    """Start the pull process on the requested entities."""
    _validate_requested(link, requested)
    return create_link_operation_result(
        entity.apply(Operations.START_PULL).operation_results[0] for entity in link if entity.identifier in requested
    )


def start_delete(link: Link, *, requested: Iterable[Identifier]) -> LinkOperationResult:
    """Start the delete process on the requested entities."""
    _validate_requested(link, requested)
    return create_link_operation_result(
        entity.apply(Operations.START_DELETE).operation_results[0] for entity in link if entity.identifier in requested
    )
