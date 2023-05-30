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

    def pull(self, entity: Entity) -> Update:  # pylint: disable=unused-argument
        """Return the commands needed to pull an entity."""
        return Update(Transition(self.__class__, self.__class__), commands=frozenset())

    def delete(self, entity: Entity) -> Update:  # pylint: disable=unused-argument
        """Return the commands needed to delete the entity."""
        return Update(Transition(self.__class__, self.__class__), commands=frozenset())

    def process(self, entity: Entity) -> Update:  # pylint: disable=unused-argument
        """Return the commands needed to process the entity."""
        return Update(Transition(self.__class__, self.__class__), commands=frozenset())

    def flag(self, entity: Entity) -> Update:  # pylint: disable=unused-argument
        """Return the commands needed to flag the entity."""
        return Update(Transition(self.__class__, self.__class__), commands=frozenset())

    def unflag(self, entity: Entity) -> Update:  # pylint: disable=unused-argument
        """Return the commands needed to unflag the entity."""
        return Update(Transition(self.__class__, self.__class__), commands=frozenset())

    def _construct_commands(
        self, identifier: Identifier, commands: Iterable[type[command.Command]]
    ) -> set[command.Command]:
        return {command(identifier) for command in commands}


class Idle(State):
    """The default state of an entity."""

    def pull(self, entity: Entity) -> Update:
        """Return the commands needed to pull an idle entity."""
        transition = Transition(self.__class__, Activated)
        return Update(
            transition, commands=frozenset(self._construct_commands(entity.identifier, TRANSITION_MAP[transition]))
        )


class Activated(State):
    """The state of an activated entity."""

    def process(self, entity: Entity) -> Update:
        """Return the commands needed to process an activated entity."""
        if entity.operation is Operations.PULL:
            transition = Transition(self.__class__, Received)
        elif entity.operation is Operations.DELETE:
            transition = Transition(self.__class__, Idle)
        return Update(
            transition, commands=frozenset(self._construct_commands(entity.identifier, TRANSITION_MAP[transition]))
        )


class Received(State):
    """The state of an received entity."""

    def process(self, entity: Entity) -> Update:
        """Return the commands needed to process a received entity."""
        if entity.operation is Operations.PULL:
            transition = Transition(self.__class__, Pulled)
        elif entity.operation is Operations.DELETE:
            transition = Transition(self.__class__, Activated)
        return Update(
            transition, commands=frozenset(self._construct_commands(entity.identifier, TRANSITION_MAP[transition]))
        )


class Pulled(State):
    """The state of an entity that has been copied to the local side."""

    def delete(self, entity: Entity) -> Update:
        """Return the commands needed to delete a pulled entity."""
        transition = Transition(self.__class__, Received)
        return Update(
            transition, commands=frozenset(self._construct_commands(entity.identifier, TRANSITION_MAP[transition]))
        )

    def flag(self, entity: Entity) -> Update:
        """Return the commands needed to flag a pulled entity."""
        transition = Transition(self.__class__, Tainted)
        return Update(
            transition, commands=frozenset(self._construct_commands(entity.identifier, TRANSITION_MAP[transition]))
        )


class Tainted(State):
    """The state of an entity that has been flagged as faulty by the source side."""

    def delete(self, entity: Entity) -> Update:
        """Return the commands needed to delete a tainted entity."""
        transition = Transition(self.__class__, Received)
        return Update(
            transition, commands=frozenset(self._construct_commands(entity.identifier, TRANSITION_MAP[transition]))
        )

    def unflag(self, entity: Entity) -> Update:
        """Return the commands needed to unflag a tainted entity."""
        transition = Transition(self.__class__, Pulled)
        return Update(
            transition, commands=frozenset(self._construct_commands(entity.identifier, TRANSITION_MAP[transition]))
        )


class Deprecated(State):
    """The state of a faulty entity that was deleted by the local side."""

    def unflag(self, entity: Entity) -> Update:
        """Return the commands to unflag a deprecated entity."""
        transition = Transition(self.__class__, Idle)
        return Update(
            transition, commands=frozenset(self._construct_commands(entity.identifier, TRANSITION_MAP[transition]))
        )


@dataclass(frozen=True)
class Transition:
    """Represents the transition between two entity states."""

    current: type[State]
    new: type[State]


TRANSITION_MAP: dict[Transition, set[type[command.Command]]] = {
    Transition(Idle, Activated): {command.AddToOutbound, command.StartPullOperation},
    Transition(Activated, Received): {command.AddToLocal},
    Transition(Activated, Idle): {command.RemoveFromOutbound, command.FinishDeleteOperation},
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
        return self.state().pull(self)

    def delete(self) -> Update:
        """Delete the entity."""
        return self.state().delete(self)

    def process(self) -> Update:
        """Process the entity."""
        return self.state().process(self)

    def flag(self) -> Update:
        """Flag the entity."""
        return self.state().flag(self)

    def unflag(self) -> Update:
        """Unflag the entity."""
        return self.state().unflag(self)
