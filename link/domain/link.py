"""Contains the link class."""
from __future__ import annotations

from typing import Any, Iterable, Iterator, Mapping, Optional, Set, Tuple, TypeVar

from . import events
from .custom_types import Identifier
from .state import (
    STATE_MAP,
    Components,
    Entity,
    Operations,
    PersistentState,
    Processes,
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


class Link(Set[Entity]):
    """The state of a link between two databases."""

    def __init__(
        self, entities: Iterable[Entity], operation_results: Tuple[events.LinkStateChanged, ...] = tuple()
    ) -> None:
        """Initialize the link."""
        self._entities = set(entities)
        self._operation_results = operation_results

    @property
    def identifiers(self) -> frozenset[Identifier]:
        """Return the identifiers of all entities in the link."""
        return frozenset(entity.identifier for entity in self)

    @property
    def operation_results(self) -> Tuple[events.LinkStateChanged, ...]:
        """Return the results of operations performed on this link."""
        return self._operation_results

    def apply(self, operation: Operations, *, requested: Iterable[Identifier]) -> Link:
        """Apply an operation to the requested entities."""

        def create_operation_result(
            results: Iterable[events.EntityOperationApplied], requested: Iterable[Identifier]
        ) -> events.LinkStateChanged:
            """Create the result of an operation on a link from results of individual entities."""
            results = set(results)
            operation = next(iter(results)).operation
            return events.LinkStateChanged(
                operation,
                requested=frozenset(requested),
                updates=frozenset(result for result in results if isinstance(result, events.EntityStateChanged)),
                errors=frozenset(result for result in results if isinstance(result, events.InvalidOperationRequested)),
            )

        assert requested, "No identifiers requested."
        assert set(requested) <= self.identifiers, "Requested identifiers not present in link."
        changed = {entity.apply(operation) for entity in self if entity.identifier in requested}
        unchanged = {entity for entity in self if entity.identifier not in requested}
        operation_results = self.operation_results + (
            create_operation_result((entity.operation_results[-1] for entity in changed), requested),
        )
        return Link(changed | unchanged, operation_results)

    def __contains__(self, entity: object) -> bool:
        """Check if the link contains the given entity."""
        return entity in self._entities

    def __iter__(self) -> Iterator[Entity]:
        """Iterate over all entities in the link."""
        return iter(self._entities)

    def __len__(self) -> int:
        """Return the number of entities in the link."""
        return len(self._entities)
