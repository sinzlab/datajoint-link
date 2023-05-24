"""Contains the link class."""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import FrozenSet, Mapping, NewType, Optional


class Components(Enum):
    """Names for the different components in a link."""

    SOURCE = 1
    OUTBOUND = 2
    LOCAL = 3


class States(Enum):
    """Names for the different states of an entity."""

    IDLE = 1
    ACTIVATED = 2
    RECEIVED = 3
    PULLED = 4
    TAINTED = 5
    DEPRECATED = 6


class Marks(Enum):
    """Names for the different marks an entity can have."""

    PULL = 1
    DELETE = 2


Identifier = NewType("Identifier", str)


@dataclass(frozen=True)
class Entity:
    """An entity in a link."""

    identifier: Identifier
    state: States
    mark: Optional[Marks] = None


@dataclass(frozen=True)
class PersistentState:
    """The persistent state of an entity."""

    presence: frozenset[Components]
    is_tainted: bool
    is_transiting: bool


STATE_MAP = {
    PersistentState(
        frozenset({Components.SOURCE}),
        is_tainted=False,
        is_transiting=False,
    ): States.IDLE,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND}),
        is_tainted=False,
        is_transiting=True,
    ): States.ACTIVATED,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND, Components.LOCAL}),
        is_tainted=False,
        is_transiting=True,
    ): States.RECEIVED,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND, Components.LOCAL}),
        is_tainted=False,
        is_transiting=False,
    ): States.PULLED,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND, Components.LOCAL}),
        is_tainted=True,
        is_transiting=False,
    ): States.TAINTED,
    PersistentState(
        frozenset({Components.SOURCE}),
        is_tainted=True,
        is_transiting=False,
    ): States.DEPRECATED,
}


def create_link(
    assignments: Mapping[Components, Iterable[Identifier]],
    *,
    tainted_identifiers: Optional[Iterable[Identifier]] = None,
    transiting_identifiers: Optional[Iterable[Identifier]] = None,
    marks: Optional[Mapping[Marks, Iterable[Identifier]]] = None,
) -> Link:
    """Create a new link instance."""

    def validate_assignments(
        assignments: Mapping[Components, Iterable[Identifier]], tainted: Iterable[Identifier]
    ) -> None:
        assert set(assignments[Components.OUTBOUND]) <= set(
            assignments[Components.SOURCE]
        ), "Outbound must not be superset of source."
        assert set(assignments[Components.LOCAL]) <= set(
            assignments[Components.OUTBOUND]
        ), "Local must not be superset of source."
        assert set(tainted) <= set(assignments[Components.SOURCE])

    def create_entities(
        assignments: Mapping[Components, Iterable[Identifier]],
        tainted: Iterable[Identifier],
        transiting_identifiers: Iterable[Identifier],
    ) -> set[Entity]:
        def create_entity(identifier: Identifier) -> Entity:
            presence = frozenset(
                component for component, identifiers in assignments.items() if identifier in identifiers
            )
            persistent_state = PersistentState(
                presence, is_tainted=identifier in tainted, is_transiting=identifier in transiting_identifiers
            )
            state = STATE_MAP[persistent_state]
            try:
                mark = marks_map[identifier]
            except KeyError:
                mark = None
            return Entity(identifier, state=state, mark=mark)

        return {create_entity(identifier) for identifier in assignments[Components.SOURCE]}

    def assign_entities(entities: Iterable[Entity]) -> dict[Components, set[Entity]]:
        def assign_to_component(component: Components) -> set[Entity]:
            return {entity for entity in entities if entity.identifier in assignments[component]}

        return {component: assign_to_component(component) for component in Components}

    if tainted_identifiers is None:
        tainted_identifiers = set()
    if transiting_identifiers is None:
        transiting_identifiers = set()
    if marks is None:
        marks = {}
    marks_map = {identifier: mark for mark, identifiers in marks.items() for identifier in identifiers}
    validate_assignments(assignments, tainted_identifiers)
    entity_assignments = assign_entities(create_entities(assignments, tainted_identifiers, transiting_identifiers))
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


def pull(
    link: Link,
    *,
    requested: Iterable[Identifier],
) -> set[Transfer]:
    """Create the transfer specifications needed for pulling the requested identifiers."""
    assert set(requested) <= link[Components.SOURCE].identifiers, "Requested must not be superset of source."
    assert all(
        entity.state is States.IDLE for entity in link[Components.SOURCE] if entity.identifier in set(requested)
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
