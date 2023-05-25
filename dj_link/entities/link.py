"""Contains the link class."""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any, FrozenSet, Mapping, NewType, Optional, TypeVar


class Components(Enum):
    """Names for the different components in a link."""

    SOURCE = 1
    OUTBOUND = 2
    LOCAL = 3


class State:  # pylint: disable=too-few-public-methods
    """An entity's state."""

    def pull(self, entity: Entity) -> set[Command]:  # pylint: disable=unused-argument
        """Return the commands needed to pull an entity."""
        return set()


class Idle(State):  # pylint: disable=too-few-public-methods
    """The default state of an entity."""

    def pull(self, entity: Entity) -> set[Command]:
        """Return the commands needed to pull an idle entity."""
        return {
            command(entity.identifier)
            for command in TRANSITION_MAP[Transition(self.__class__, Activated, operation=entity.operation)]
        }


class Activated(State):  # pylint: disable=too-few-public-methods
    """The state of an activated entity."""

    def pull(self, entity: Entity) -> set[Command]:
        """Return the commands needed to pull an activated entity."""
        return {
            command(entity.identifier)
            for command in TRANSITION_MAP[Transition(self.__class__, Received, operation=entity.operation)]
        }


class Received(State):  # pylint: disable=too-few-public-methods
    """The state of an received entity."""

    def pull(self, entity: Entity) -> set[Command]:
        """Return the commands needed to pull a received entity."""
        return {
            command(entity.identifier)
            for command in TRANSITION_MAP[Transition(self.__class__, Pulled, operation=entity.operation)]
        }


class Pulled(State):  # pylint: disable=too-few-public-methods
    """The state of an entity that has been copied to the local side."""


class Tainted(State):  # pylint: disable=too-few-public-methods
    """The state of an entity that has been flagged as faulty by the source side."""


class Deprecated(State):  # pylint: disable=too-few-public-methods
    """The state of a faulty entity that was deleted by the local side."""


class Operations(Enum):
    """Names for operations that pull/delete entities into/from the local side."""

    PULL = 1
    DELETE = 2


Identifier = NewType("Identifier", str)


@dataclass(frozen=True)
class Entity:
    """An entity in a link."""

    identifier: Identifier
    state: type[State]
    operation: Optional[Operations]

    def pull(self) -> set[Command]:
        """Pull the entity."""
        return self.state().pull(self)


@dataclass(frozen=True)
class PersistentState:
    """The persistent state of an entity."""

    presence: frozenset[Components]
    is_tainted: bool
    has_operation: bool


STATE_MAP = {
    PersistentState(
        frozenset({Components.SOURCE}),
        is_tainted=False,
        has_operation=False,
    ): Idle,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND}),
        is_tainted=False,
        has_operation=True,
    ): Activated,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND, Components.LOCAL}),
        is_tainted=False,
        has_operation=True,
    ): Received,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND, Components.LOCAL}),
        is_tainted=False,
        has_operation=False,
    ): Pulled,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND, Components.LOCAL}),
        is_tainted=True,
        has_operation=False,
    ): Tainted,
    PersistentState(
        frozenset({Components.SOURCE}),
        is_tainted=True,
        has_operation=False,
    ): Deprecated,
}


@dataclass(frozen=True)
class Transition:
    """A transition between two entity states."""

    current: type[State]
    next: type[State]
    operation: Optional[Operations]


@dataclass(frozen=True)
class Command:
    """A command to be executed by the persistence layer and produced by an entity undergoing a state transition."""

    identifier: Identifier


@dataclass(frozen=True)
class AddToOutbound(Command):
    """A command to add an entity to the outbound component."""


@dataclass(frozen=True)
class AddToLocal(Command):
    """A command to add an entity to the outbound component."""


@dataclass(frozen=True)
class MarkAsPulled(Command):
    """A command to mark an entity as currently undergoing a pull."""


@dataclass(frozen=True)
class FinishPullOperation(Command):
    """A command finishing the pull operation on an entity."""


TRANSITION_MAP: dict[Transition, set[type[Command]]] = {
    Transition(Idle, Activated, operation=None): {AddToOutbound, MarkAsPulled},
    Transition(Activated, Received, operation=Operations.PULL): {AddToLocal},
    Transition(Received, Pulled, operation=Operations.PULL): {FinishPullOperation},
}


def create_link(
    assignments: Mapping[Components, Iterable[Identifier]],
    *,
    tainted_identifiers: Optional[Iterable[Identifier]] = None,
    operations: Optional[Mapping[Operations, Iterable[Identifier]]] = None,
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
        operations: Mapping[Operations, Iterable[Identifier]],
    ) -> None:
        assert set(assignments[Components.OUTBOUND]) <= set(
            assignments[Components.SOURCE]
        ), "Outbound must not be superset of source."
        assert set(assignments[Components.LOCAL]) <= set(
            assignments[Components.OUTBOUND]
        ), "Local must not be superset of source."
        assert set(tainted) <= set(assignments[Components.SOURCE])
        assert pairwise_disjoint(operations.values()), "Identifiers can not undergo more than one operation."

    def create_entities(
        assignments: Mapping[Components, Iterable[Identifier]],
        tainted: Iterable[Identifier],
    ) -> set[Entity]:
        def create_entity(identifier: Identifier) -> Entity:
            presence = frozenset(
                component for component, identifiers in assignments.items() if identifier in identifiers
            )
            persistent_state = PersistentState(
                presence, is_tainted=identifier in tainted, has_operation=identifier in operations_map
            )
            state = STATE_MAP[persistent_state]
            return Entity(identifier, state=state, operation=operations_map.get(identifier))

        return {create_entity(identifier) for identifier in assignments[Components.SOURCE]}

    def assign_entities(entities: Iterable[Entity]) -> dict[Components, set[Entity]]:
        def assign_to_component(component: Components) -> set[Entity]:
            return {entity for entity in entities if entity.identifier in assignments[component]}

        return {component: assign_to_component(component) for component in Components}

    if tainted_identifiers is None:
        tainted_identifiers = set()
    if operations is None:
        operations = {}
    validate_arguments(assignments, tainted_identifiers, operations)
    operations_map = invert_mapping(operations)
    entity_assignments = assign_entities(create_entities(assignments, tainted_identifiers))
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
        entity.state is Idle for entity in link[Components.SOURCE] if entity.identifier in set(requested)
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
