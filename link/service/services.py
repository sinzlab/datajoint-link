"""Contains all the services."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto

from link.domain import commands, events
from link.domain.custom_types import Identifier
from link.domain.state import Operations, states

from .uow import UnitOfWork


class Response:
    """Base class for all response models."""


@dataclass(frozen=True)
class PullResponse(Response):
    """Response model for the pull use-case."""

    requested: frozenset[Identifier]
    errors: frozenset[events.InvalidOperationRequested]


def pull(command: commands.PullEntities, *, uow: UnitOfWork, output_port: Callable[[PullResponse], None]) -> None:
    """Pull entities across the link."""
    with uow:
        link = uow.link.pull(command.requested)
        uow.commit()
    state_changed_events = (event for event in link.events if isinstance(event, events.LinkStateChanged))
    start_pull_event = next(event for event in state_changed_events if event.operation is Operations.START_PULL)
    errors = (error for error in start_pull_event.errors if error.state is states.Deprecated)
    output_port(PullResponse(command.requested, frozenset(errors)))


@dataclass(frozen=True)
class DeleteResponse(Response):
    """Response model for the delete use-case."""

    requested: frozenset[Identifier]


def delete(command: commands.DeleteEntities, *, uow: UnitOfWork, output_port: Callable[[DeleteResponse], None]) -> None:
    """Delete pulled entities."""
    with uow:
        uow.link.delete(command.requested)
        uow.commit()
    output_port(DeleteResponse(command.requested))


def list_idle_entities(
    command: commands.ListIdleEntities,
    *,
    uow: UnitOfWork,
    output_port: Callable[[events.IdleEntitiesListed], None],
) -> None:
    """List all idle entities."""
    with uow:
        uow.link.list_idle_entities()
        event = uow.link.events[-1]
        assert isinstance(event, events.IdleEntitiesListed)
        output_port(event)


class Services(Enum):
    """Names for all available services."""

    PULL = auto()
    DELETE = auto()
    PROCESS = auto()
    LIST_IDLE_ENTITIES = auto()
