"""Contains everything state related."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, replace
from enum import Enum, auto
from functools import partial

from .custom_types import Identifier
from .events import EntityOperationApplied, EntityStateChanged, InvalidOperationRequested


class State:
    """An entity's state."""

    @classmethod
    def start_pull(cls, entity: Entity) -> Entity:
        """Return the command needed to start the pull process for the entity."""
        return cls._create_invalid_operation(entity, Operations.START_PULL)

    @classmethod
    def start_delete(cls, entity: Entity) -> Entity:
        """Return the commands needed to start the delete process for the entity."""
        return cls._create_invalid_operation(entity, Operations.START_DELETE)

    @classmethod
    def process(cls, entity: Entity) -> Entity:
        """Return the commands needed to process the entity."""
        return cls._create_invalid_operation(entity, Operations.PROCESS)

    @staticmethod
    def _create_invalid_operation(entity: Entity, operation: Operations) -> Entity:
        entity.events.append(InvalidOperationRequested(operation, entity.identifier, entity.state))
        return replace(entity)

    @classmethod
    def _transition_entity(
        cls, entity: Entity, operation: Operations, new_state: type[State], *, new_process: Processes | None = None
    ) -> Entity:
        if new_process is None:
            new_process = entity.current_process
        transition = Transition(cls, new_state)
        entity.events.append(
            EntityStateChanged(operation, entity.identifier, transition, TRANSITION_MAP[transition]),
        )
        return replace(entity, state=transition.new, current_process=new_process)


class States:
    """A namespace containing all states."""

    def __init__(self) -> None:
        """Initialize the namespace."""
        self.__states: dict[str, type[State]] = {}

    def __getattr__(self, name: str) -> type[State]:
        """Return the state corresponding to the given name."""
        return self.__states[name]

    def register(self, state: type[State]) -> None:
        """Add the given state to the namespace."""
        self.__states[state.__name__] = state


states = States()


class Idle(State):
    """The default state of an entity."""

    @classmethod
    def start_pull(cls, entity: Entity) -> Entity:
        """Return the command needed to start the pull process for an entity."""
        return cls._transition_entity(entity, Operations.START_PULL, Activated, new_process=Processes.PULL)


states.register(Idle)


class Activated(State):
    """The state of an activated entity."""

    @classmethod
    def process(cls, entity: Entity) -> Entity:
        """Return the commands needed to process an activated entity."""
        transition_entity = partial(cls._transition_entity, entity, Operations.PROCESS)
        if entity.is_tainted:
            return transition_entity(Deprecated, new_process=Processes.NONE)
        elif entity.current_process is Processes.PULL:
            return transition_entity(Received)
        elif entity.current_process is Processes.DELETE:
            return transition_entity(Idle, new_process=Processes.NONE)
        raise RuntimeError


states.register(Activated)


class Received(State):
    """The state of an received entity."""

    @classmethod
    def process(cls, entity: Entity) -> Entity:
        """Return the commands needed to process a received entity."""
        transition_entity = partial(cls._transition_entity, entity, Operations.PROCESS)
        if entity.current_process is Processes.PULL:
            if entity.is_tainted:
                return transition_entity(Tainted, new_process=Processes.NONE)
            else:
                return transition_entity(Pulled, new_process=Processes.NONE)
        elif entity.current_process is Processes.DELETE:
            return transition_entity(Activated)
        raise RuntimeError


states.register(Received)


class Pulled(State):
    """The state of an entity that has been copied to the local side."""

    @classmethod
    def start_delete(cls, entity: Entity) -> Entity:
        """Return the commands needed to start the delete process for the entity."""
        return cls._transition_entity(entity, Operations.START_DELETE, Received, new_process=Processes.DELETE)


states.register(Pulled)


class Tainted(State):
    """The state of an entity that has been flagged as faulty by the source side."""

    @classmethod
    def start_delete(cls, entity: Entity) -> Entity:
        """Return the commands needed to start the delete process for the entity."""
        return cls._transition_entity(entity, Operations.START_DELETE, Received, new_process=Processes.DELETE)


states.register(Tainted)


class Deprecated(State):
    """The state of a faulty entity that was deleted by the local side."""


states.register(Deprecated)


@dataclass(frozen=True)
class Transition:
    """Represents the transition between two entity states."""

    current: type[State]
    new: type[State]

    def __post_init__(self) -> None:
        """Validate the transition."""
        assert self.current is not self.new, "Current and new state are identical."


class Commands(Enum):
    """Names for all the commands necessary to transition entities between states."""

    ADD_TO_LOCAL = auto()
    REMOVE_FROM_LOCAL = auto()
    START_PULL_PROCESS = auto()
    FINISH_PULL_PROCESS = auto()
    START_DELETE_PROCESS = auto()
    FINISH_DELETE_PROCESS = auto()
    DEPRECATE = auto()


TRANSITION_MAP: dict[Transition, Commands] = {
    Transition(Idle, Activated): Commands.START_PULL_PROCESS,
    Transition(Activated, Received): Commands.ADD_TO_LOCAL,
    Transition(Activated, Idle): Commands.FINISH_DELETE_PROCESS,
    Transition(Activated, Deprecated): Commands.DEPRECATE,
    Transition(Received, Pulled): Commands.FINISH_PULL_PROCESS,
    Transition(Received, Tainted): Commands.FINISH_PULL_PROCESS,
    Transition(Received, Activated): Commands.REMOVE_FROM_LOCAL,
    Transition(Pulled, Received): Commands.START_DELETE_PROCESS,
    Transition(Tainted, Received): Commands.START_DELETE_PROCESS,
}


class Operations(Enum):
    """Names for all operations that can be performed on entities."""

    START_PULL = auto()
    START_DELETE = auto()
    PROCESS = auto()


class Processes(Enum):
    """Names for processes that pull/delete entities into/from the local side."""

    NONE = auto()
    PULL = auto()
    DELETE = auto()


class Components(Enum):
    """Names for the different components in a link."""

    SOURCE = 1
    OUTBOUND = 2
    LOCAL = 3


@dataclass(frozen=True)
class PersistentState:
    """The persistent state of an entity."""

    presence: frozenset[Components]
    is_tainted: bool
    has_process: bool


STATE_MAP = {
    PersistentState(
        frozenset({Components.SOURCE}),
        is_tainted=False,
        has_process=False,
    ): Idle,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND}),
        is_tainted=False,
        has_process=True,
    ): Activated,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND}),
        is_tainted=True,
        has_process=True,
    ): Activated,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND, Components.LOCAL}),
        is_tainted=False,
        has_process=True,
    ): Received,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND, Components.LOCAL}),
        is_tainted=True,
        has_process=True,
    ): Received,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND, Components.LOCAL}),
        is_tainted=False,
        has_process=False,
    ): Pulled,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND, Components.LOCAL}),
        is_tainted=True,
        has_process=False,
    ): Tainted,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND}),
        is_tainted=True,
        has_process=False,
    ): Deprecated,
}


@dataclass
class Entity:
    """An entity in a link."""

    identifier: Identifier
    state: type[State]
    current_process: Processes
    is_tainted: bool
    events: deque[EntityOperationApplied]

    def apply(self, operation: Operations) -> Entity:
        """Apply an operation to the entity."""
        if operation is Operations.START_PULL:
            return self._start_pull()
        if operation is Operations.START_DELETE:
            return self._start_delete()
        if operation is Operations.PROCESS:
            return self._process()

    def _start_pull(self) -> Entity:
        """Start the pull process for the entity."""
        return self.state.start_pull(self)

    def _start_delete(self) -> Entity:
        """Start the delete process for the entity."""
        return self.state.start_delete(self)

    def _process(self) -> Entity:
        """Process the entity."""
        return self.state.process(self)

    def __hash__(self) -> int:
        """Return the hash of this entity."""
        return hash(self.identifier)

    def __eq__(self, other: object) -> bool:
        """Return True if both entities are equal."""
        if not isinstance(other, type(self)):
            raise NotImplementedError
        return hash(self) == hash(other)
