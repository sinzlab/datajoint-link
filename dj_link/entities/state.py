"""Contains everything state related."""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from . import command
from .custom_types import Identifier


class State:
    """An entity's state."""

    @classmethod
    def pull(cls, entity: Entity) -> Update:
        """Return the commands needed to pull an entity."""
        return cls._create_no_transition_update(entity.identifier)

    @classmethod
    def delete(cls, entity: Entity) -> Update:
        """Return the commands needed to delete the entity."""
        return cls._create_no_transition_update(entity.identifier)

    @classmethod
    def process(cls, entity: Entity) -> Update:
        """Return the commands needed to process the entity."""
        return cls._create_no_transition_update(entity.identifier)

    @classmethod
    def flag(cls, entity: Entity) -> Update:
        """Return the commands needed to flag the entity."""
        return cls._create_no_transition_update(entity.identifier)

    @classmethod
    def unflag(cls, entity: Entity) -> Update:
        """Return the commands needed to unflag the entity."""
        return cls._create_no_transition_update(entity.identifier)

    @classmethod
    def _create_no_transition_update(cls, identifier: Identifier) -> Update:
        return cls._create_update(identifier, cls)

    @classmethod
    def _create_update(cls, identifier: Identifier, new_state: type[State]) -> Update:
        def create_commands(identifier: Identifier, commands: Iterable[type[command.Command]]) -> set[command.Command]:
            return {command(identifier) for command in commands}

        transition = Transition(cls, new_state)
        return Update(
            transition, commands=frozenset(create_commands(identifier, TRANSITION_MAP.get(transition, set())))
        )


class Idle(State):
    """The default state of an entity."""

    @classmethod
    def pull(cls, entity: Entity) -> Update:
        """Return the commands needed to pull an idle entity."""
        return cls._create_update(entity.identifier, Activated)


class Activated(State):
    """The state of an activated entity."""

    @classmethod
    def process(cls, entity: Entity) -> Update:
        """Return the commands needed to process an activated entity."""
        new_state: type[State]
        if entity.operation is Operations.PULL:
            new_state = Received
        elif entity.operation is Operations.DELETE:
            if entity.is_tainted:
                new_state = Deprecated
            else:
                new_state = Idle
        return cls._create_update(entity.identifier, new_state)


class Received(State):
    """The state of an received entity."""

    @classmethod
    def process(cls, entity: Entity) -> Update:
        """Return the commands needed to process a received entity."""
        new_state: type[State]
        if entity.operation is Operations.PULL:
            new_state = Pulled
        elif entity.operation is Operations.DELETE:
            new_state = Activated
        return cls._create_update(entity.identifier, new_state)


class Pulled(State):
    """The state of an entity that has been copied to the local side."""

    @classmethod
    def delete(cls, entity: Entity) -> Update:
        """Return the commands needed to delete a pulled entity."""
        return cls._create_update(entity.identifier, Received)

    @classmethod
    def flag(cls, entity: Entity) -> Update:
        """Return the commands needed to flag a pulled entity."""
        return cls._create_update(entity.identifier, Tainted)


class Tainted(State):
    """The state of an entity that has been flagged as faulty by the source side."""

    @classmethod
    def delete(cls, entity: Entity) -> Update:
        """Return the commands needed to delete a tainted entity."""
        return cls._create_update(entity.identifier, Received)

    @classmethod
    def unflag(cls, entity: Entity) -> Update:
        """Return the commands needed to unflag a tainted entity."""
        return cls._create_update(entity.identifier, Pulled)


class Deprecated(State):
    """The state of a faulty entity that was deleted by the local side."""

    @classmethod
    def unflag(cls, entity: Entity) -> Update:
        """Return the commands to unflag a deprecated entity."""
        return cls._create_update(entity.identifier, Idle)


@dataclass(frozen=True)
class Transition:
    """Represents the transition between two entity states."""

    current: type[State]
    new: type[State]


TRANSITION_MAP: dict[Transition, set[type[command.Command]]] = {
    Transition(Idle, Activated): {command.AddToOutbound, command.StartPullOperation},
    Transition(Activated, Received): {command.AddToLocal},
    Transition(Activated, Idle): {command.RemoveFromOutbound, command.FinishDeleteOperation},
    Transition(Activated, Deprecated): {command.RemoveFromOutbound, command.FinishDeleteOperation},
    Transition(Received, Pulled): {command.FinishPullOperation},
    Transition(Received, Activated): {command.RemoveFromLocal},
    Transition(Pulled, Received): {command.StartDeleteOperation},
    Transition(Pulled, Tainted): {command.Flag},
    Transition(Tainted, Pulled): {command.Unflag},
    Transition(Tainted, Received): {command.StartDeleteOperation},
    Transition(Deprecated, Idle): {command.Unflag},
}


@dataclass(frozen=True)
class Update:
    """Represents the persistent update needed to transition an entity."""

    transition: Transition
    commands: frozenset[command.Command]


class Operations(Enum):
    """Names for operations that pull/delete entities into/from the local side."""

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
        frozenset({Components.SOURCE, Components.OUTBOUND}),
        is_tainted=True,
        has_operation=True,
    ): Activated,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND, Components.LOCAL}),
        is_tainted=False,
        has_operation=True,
    ): Received,
    PersistentState(
        frozenset({Components.SOURCE, Components.OUTBOUND, Components.LOCAL}),
        is_tainted=True,
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
class Entity:
    """An entity in a link."""

    identifier: Identifier
    state: type[State]
    operation: Optional[Operations]
    is_tainted: bool

    def pull(self) -> Update:
        """Pull the entity."""
        return self.state.pull(self)

    def delete(self) -> Update:
        """Delete the entity."""
        return self.state.delete(self)

    def process(self) -> Update:
        """Process the entity."""
        return self.state.process(self)

    def flag(self) -> Update:
        """Flag the entity."""
        return self.state.flag(self)

    def unflag(self) -> Update:
        """Unflag the entity."""
        return self.state.unflag(self)
