"""Contains everything state related."""
from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum, auto
from typing import Optional, Union

from .custom_types import Identifier


class State:
    """An entity's state."""

    @classmethod
    def start_pull(cls, entity: Entity) -> Entity:
        """Return the command needed to start the pull process for the entity."""
        return entity

    @classmethod
    def start_delete(cls, entity: Entity) -> Entity:
        """Return the commands needed to start the delete process for the entity."""
        return entity

    @classmethod
    def process(cls, entity: Entity) -> Entity:
        """Return the commands needed to process the entity."""
        return entity


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
        return replace(entity, state=Activated, current_process=Processes.PULL)


states.register(Idle)


class Activated(State):
    """The state of an activated entity."""

    @classmethod
    def process(cls, entity: Entity) -> Entity:
        """Return the commands needed to process an activated entity."""
        if entity.is_tainted:
            return replace(entity, state=Deprecated, current_process=None)
        elif entity.current_process is Processes.PULL:
            return replace(entity, state=Received)
        elif entity.current_process is Processes.DELETE:
            return replace(entity, state=Idle, current_process=None)
        raise RuntimeError


states.register(Activated)


class Received(State):
    """The state of an received entity."""

    @classmethod
    def process(cls, entity: Entity) -> Entity:
        """Return the commands needed to process a received entity."""
        if entity.current_process is Processes.PULL:
            if entity.is_tainted:
                return replace(entity, state=Tainted, current_process=None)
            else:
                return replace(entity, state=Pulled, current_process=None)
        elif entity.current_process is Processes.DELETE:
            return replace(entity, state=Activated)
        raise RuntimeError


states.register(Received)


class Pulled(State):
    """The state of an entity that has been copied to the local side."""

    @classmethod
    def start_delete(cls, entity: Entity) -> Entity:
        """Return the commands needed to start the delete process for the entity."""
        return replace(entity, state=Received, current_process=Processes.DELETE)


states.register(Pulled)


class Tainted(State):
    """The state of an entity that has been flagged as faulty by the source side."""

    @classmethod
    def start_delete(cls, entity: Entity) -> Entity:
        """Return the commands needed to start the delete process for the entity."""
        return replace(entity, state=Received, current_process=Processes.DELETE)


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


@dataclass(frozen=True)
class Update:
    """Represents the persistent update needed to transition an entity."""

    operation: Operations
    identifier: Identifier
    transition: Transition
    command: Commands


@dataclass(frozen=True)
class InvalidOperation:
    """Represents the result of attempting an operation that is invalid in the entity's current state."""

    operation: Operations
    identifier: Identifier
    state: type[State]


EntityOperationResult = Union[Update, InvalidOperation]


class Processes(Enum):
    """Names for processes that pull/delete entities into/from the local side."""

    PULL = 1
    DELETE = 2


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


@dataclass(frozen=True)
class Entity:
    """An entity in a link."""

    identifier: Identifier
    state: type[State]
    current_process: Optional[Processes]
    is_tainted: bool

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
