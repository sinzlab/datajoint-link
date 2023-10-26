"""Contains all the services."""
from __future__ import annotations

from collections.abc import Callable
from enum import Enum, auto

from link.domain import commands, events

from .uow import UnitOfWork


def pull(
    command: commands.PullEntities, *, uow: UnitOfWork, output_port: Callable[[events.EntitiesPulled], None]
) -> None:
    """Pull entities across the link."""
    with uow:
        link = uow.link.pull(command.requested)
        uow.commit()
    event = link.events[-1]
    assert isinstance(event, events.EntitiesPulled)
    output_port(event)


def delete(
    command: commands.DeleteEntities, *, uow: UnitOfWork, output_port: Callable[[events.EntitiesDeleted], None]
) -> None:
    """Delete pulled entities."""
    with uow:
        link = uow.link.delete(command.requested)
        uow.commit()
    event = link.events[-1]
    assert isinstance(event, events.EntitiesDeleted)
    output_port(event)


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
    LIST_IDLE_ENTITIES = auto()
