"""Contains code handling domain commands and events."""
from __future__ import annotations

from collections.abc import Callable

from link.domain import commands, events
from link.domain.state import Processes

from .messagebus import MessageBus
from .progress import ProgessDisplay
from .uow import UnitOfWork


def pull_entity(command: commands.PullEntity, *, uow: UnitOfWork, message_bus: MessageBus) -> None:
    """Pull an entity across the link."""
    message_bus.handle(events.ProcessStarted(Processes.PULL, command.requested))
    with uow:
        uow.link[command.requested].pull()
        uow.commit()
    message_bus.handle(events.ProcessFinished(Processes.PULL, command.requested))


def delete_entity(command: commands.DeleteEntity, *, uow: UnitOfWork, message_bus: MessageBus) -> None:
    """Delete a pulled entity."""
    message_bus.handle(events.ProcessStarted(Processes.DELETE, command.requested))
    with uow:
        uow.link[command.requested].delete()
        uow.commit()
    message_bus.handle(events.ProcessFinished(Processes.DELETE, command.requested))


def pull(command: commands.PullEntities, *, message_bus: MessageBus) -> None:
    """Pull entities across the link."""
    message_bus.handle(events.ProcessesStarted(Processes.PULL, command.requested))
    for identifier in command.requested:
        message_bus.handle(commands.PullEntity(identifier))
    message_bus.handle(events.ProcessesFinished(Processes.PULL, command.requested))


def delete(command: commands.DeleteEntities, *, message_bus: MessageBus) -> None:
    """Delete pulled entities."""
    message_bus.handle(events.ProcessesStarted(Processes.DELETE, command.requested))
    for identifier in command.requested:
        message_bus.handle(commands.DeleteEntity(identifier))
    message_bus.handle(events.ProcessesFinished(Processes.DELETE, command.requested))


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


def start_displaying_progress(event: events.ProcessesStarted, *, display: ProgessDisplay) -> None:
    """Start displaying progress to the user."""
    display.start(event.process, event.identifiers)


def inform_of_started_process(event: events.ProcessStarted, *, display: ProgessDisplay) -> None:
    """Update the display with the entity whose process started."""
    display.update_current(event.identifier)


def inform_of_finished_process(event: events.ProcessFinished, *, display: ProgessDisplay) -> None:
    """Inform the user of an entity finishing its process."""
    display.finish_current()


def stop_displaying_progress(event: events.ProcessesFinished, *, display: ProgessDisplay) -> None:
    """Stop displaying progress to the user."""
    display.stop()
