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

    def pull(self, entity: Entity) -> set[command.Command]:  # pylint: disable=unused-argument
        """Return the commands needed to pull an entity."""
        return set()

    def delete(self, entity: Entity) -> set[command.Command]:  # pylint: disable=unused-argument
        """Return the commands needed to delete the entity."""
        return set()

    def process(self, entity: Entity) -> set[command.Command]:  # pylint: disable=unused-argument
        """Return the commands needed to process the entity."""
        return set()

    def flag(self, entity: Entity) -> set[command.Command]:  # pylint: disable=unused-argument
        """Return the commands needed to flag the entity."""
        return set()

    def unflag(self, entity: Entity) -> set[command.Command]:  # pylint: disable=unused-argument
        """Return the commands needed to unflag the entity."""
        return set()

    def _construct_commands(
        self, identifier: Identifier, commands: Iterable[type[command.Command]]
    ) -> set[command.Command]:
        return {command(identifier) for command in commands}


class Idle(State):
    """The default state of an entity."""

    def pull(self, entity: Entity) -> set[command.Command]:
        """Return the commands needed to pull an idle entity."""
        return self._construct_commands(entity.identifier, (command.AddToOutbound, command.StartPullOperation))


class Activated(State):
    """The state of an activated entity."""

    def process(self, entity: Entity) -> set[command.Command]:
        """Return the commands needed to process an activated entity."""
        commands: tuple[type[command.Command], ...]
        if entity.operation is Operations.PULL:
            commands = (command.AddToLocal,)
        elif entity.operation is Operations.DELETE:
            commands = (command.RemoveFromOutbound, command.FinishDeleteOperation)
        return self._construct_commands(entity.identifier, commands)


class Received(State):
    """The state of an received entity."""

    def process(self, entity: Entity) -> set[command.Command]:
        """Return the commands needed to process a received entity."""
        commands: tuple[type[command.Command], ...]
        if entity.operation is Operations.PULL:
            commands = (command.FinishPullOperation,)
        elif entity.operation is Operations.DELETE:
            commands = (command.RemoveFromLocal,)
        return self._construct_commands(entity.identifier, commands)


class Pulled(State):
    """The state of an entity that has been copied to the local side."""

    def delete(self, entity: Entity) -> set[command.Command]:
        """Return the commands needed to delete a pulled entity."""
        return self._construct_commands(entity.identifier, (command.StartDeleteOperation,))

    def flag(self, entity: Entity) -> set[command.Command]:
        """Return the commands needed to flag a pulled entity."""
        return self._construct_commands(entity.identifier, (command.Flag,))


class Tainted(State):
    """The state of an entity that has been flagged as faulty by the source side."""

    def delete(self, entity: Entity) -> set[command.Command]:
        """Return the commands needed to delete a tainted entity."""
        return self._construct_commands(entity.identifier, (command.StartDeleteOperation,))

    def unflag(self, entity: Entity) -> set[command.Command]:
        """Return the commands needed to unflag a tainted entity."""
        return self._construct_commands(entity.identifier, (command.Unflag,))


class Deprecated(State):
    """The state of a faulty entity that was deleted by the local side."""

    def unflag(self, entity: Entity) -> set[command.Command]:
        """Return the commands to unflag a deprecated entity."""
        return self._construct_commands(entity.identifier, (command.Unflag,))


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
class Entity:
    """An entity in a link."""

    identifier: Identifier
    state: type[State]
    operation: Optional[Operations]

    def pull(self) -> set[command.Command]:
        """Pull the entity."""
        return self.state().pull(self)

    def delete(self) -> set[command.Command]:
        """Delete the entity."""
        return self.state().delete(self)

    def process(self) -> set[command.Command]:
        """Process the entity."""
        return self.state().process(self)

    def flag(self) -> set[command.Command]:
        """Flag the entity."""
        return self.state().flag(self)

    def unflag(self) -> set[command.Command]:
        """Unflag the entity."""
        return self.state().unflag(self)