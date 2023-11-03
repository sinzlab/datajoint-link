"""Contains the message bus."""
from __future__ import annotations

import logging
from collections import deque
from typing import Callable, Iterable, Protocol, TypeVar, Union

from link.domain.commands import Command
from link.domain.events import Event

from .uow import UnitOfWork

Message = Union[Command, Event]


logger = logging.getLogger()

T = TypeVar("T", bound=Command)


class CommandHandlers(Protocol):
    """A mapping of command types to handlers."""

    def __getitem__(self, command_type: type[T]) -> Callable[[T], None]:
        """Get the appropriate handler for the given type of command."""

    def __setitem__(self, command_type: type[T], handler: Callable[[T], None]) -> None:
        """Set the appropriate handler for the given type of command."""


V = TypeVar("V", bound=Event)


class EventHandlers(Protocol):
    """A mapping of event types to handlers."""

    def __getitem__(self, event_type: type[V]) -> Iterable[Callable[[V], None]]:
        """Get the appropriate handlers for the given type of event."""

    def __setitem__(selG, event_type: type[V], handlers: Iterable[Callable[[V], None]]) -> None:
        """Set the appropriate handlers for the given type of event."""


class MessageBus:
    """A message bus that dispatches domain messages to their appropriate handlers."""

    def __init__(self, uow: UnitOfWork, command_handlers: CommandHandlers, event_handlers: EventHandlers) -> None:
        """Initialize the bus."""
        self._uow = uow
        self._queue: deque[Message] = deque()
        self._command_handlers = command_handlers
        self._event_handlers = event_handlers

    def handle(self, message: Message) -> None:
        """Handle the message."""
        self._queue.append(message)
        while self._queue:
            message = self._queue.popleft()
            if isinstance(message, Command):
                self._handle_command(message)
            elif isinstance(message, Event):
                self._handle_event(message)
            else:
                raise TypeError(f"Unknown message type {type(message)!r}")

    def _handle_command(self, command: Command) -> None:
        handler = self._command_handlers[type(command)]
        try:
            handler(command)
        except Exception:
            logger.exception(f"Error handling command {command!r} with handler {handler!r}")
            raise

    def _handle_event(self, event: Event) -> None:
        for handler in self._event_handlers[type(event)]:
            try:
                handler(event)
            except Exception:
                logger.exception(f"Error handling event {event!r} with handler {handler!r}")
