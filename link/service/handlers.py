"""Contains code handling domain commands and events."""
from __future__ import annotations

from collections.abc import Callable

from link.domain import commands, events
from link.domain.state import Processes

from . import ensure
from .messagebus import MessageBus
from .progress import ProgessDisplay
from .uow import UnitOfWork


def pull_entity(command: commands.PullEntity, *, uow: UnitOfWork, message_bus: MessageBus) -> None:
    """Pull an entity across the link."""
    message_bus.handle(events.ProcessStarted(Processes.PULL, command.requested))
    with uow:
        uow.entities.create_entity(command.requested).pull()
        uow.commit()
    message_bus.handle(events.ProcessFinished(Processes.PULL, command.requested))


def delete_entity(command: commands.DeleteEntity, *, uow: UnitOfWork, message_bus: MessageBus) -> None:
    """Delete a shared entity."""
    message_bus.handle(events.ProcessStarted(Processes.DELETE, command.requested))
    with uow:
        uow.entities.create_entity(command.requested).delete()
        uow.commit()
    message_bus.handle(events.ProcessFinished(Processes.DELETE, command.requested))


def pull(command: commands.PullEntities, *, message_bus: MessageBus) -> None:
    """Pull entities across the link."""
    ensure.requests_entities(command)
    message_bus.handle(events.BatchProcessingStarted(Processes.PULL, command.requested))
    for identifier in command.requested:
        message_bus.handle(commands.PullEntity(identifier))
    message_bus.handle(events.BatchProcessingFinished(Processes.PULL, command.requested))


def delete(command: commands.DeleteEntities, *, message_bus: MessageBus) -> None:
    """Delete shared entities."""
    ensure.requests_entities(command)
    message_bus.handle(events.BatchProcessingStarted(Processes.DELETE, command.requested))
    for identifier in command.requested:
        message_bus.handle(commands.DeleteEntity(identifier))
    message_bus.handle(events.BatchProcessingFinished(Processes.DELETE, command.requested))


def log_state_change(event: events.StateChanged, log: Callable[[events.StateChanged], None]) -> None:
    """Log the state change of an entity."""
    log(event)


def inform_batch_processing_started(event: events.BatchProcessingStarted, *, display: ProgessDisplay) -> None:
    """Inform the user that batch processing started."""
    display.start(event.process, event.identifiers)


def inform_next_process_started(event: events.ProcessStarted, *, display: ProgessDisplay) -> None:
    """Inform the user that the next entity started processing."""
    display.update_current(event.identifier)


def inform_current_process_finished(event: events.ProcessFinished, *, display: ProgessDisplay) -> None:
    """Inform the user that the current entity finished processing."""
    display.finish_current()


def inform_batch_processing_finished(event: events.BatchProcessingFinished, *, display: ProgessDisplay) -> None:
    """Inform the user that batch processing finished."""
    display.stop()
