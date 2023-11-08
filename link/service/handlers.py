"""Contains code handling domain commands and events."""
from __future__ import annotations

from collections.abc import Callable

from link.domain import commands, events

from .messagebus import MessageBus
from .uow import UnitOfWork


def pull_entity(command: commands.PullEntity, *, uow: UnitOfWork) -> None:
    """Pull an entity across the link."""
    with uow:
        uow.link[command.requested].pull()
        uow.commit()


def delete_entity(command: commands.DeleteEntity, *, uow: UnitOfWork) -> None:
    """Delete a pulled entity."""
    with uow:
        uow.link[command.requested].delete()
        uow.commit()


def pull(command: commands.PullEntities, *, message_bus: MessageBus) -> None:
    """Pull entities across the link."""
    for identifier in command.requested:
        message_bus.handle(commands.PullEntity(identifier))


def delete(command: commands.DeleteEntities, *, message_bus: MessageBus) -> None:
    """Delete pulled entities."""
    for identifier in command.requested:
        message_bus.handle(commands.DeleteEntity(identifier))


def list_idle_entities(
    command: commands.ListIdleEntities,
    *,
    uow: UnitOfWork,
    output_port: Callable[[events.IdleEntitiesListed], None],
) -> None:
    """List all idle entities."""
    with uow:
        idle = uow.link.list_idle_entities()
        output_port(events.IdleEntitiesListed(idle))


def log_state_change(event: events.StateChanged, log: Callable[[events.StateChanged], None]) -> None:
    """Log the state change of an entity."""
    log(event)
