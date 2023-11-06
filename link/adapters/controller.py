"""Contains code controlling the execution of use-cases."""
from __future__ import annotations

from typing import Iterable

from link.domain import commands
from link.service.messagebus import MessageBus

from .custom_types import PrimaryKey
from .identification import IdentificationTranslator


class DJController:
    """Controls the execution of use-cases from DataJoint."""

    def __init__(
        self,
        message_bus: MessageBus,
        translator: IdentificationTranslator,
    ) -> None:
        """Initialize the translator."""
        self._message_bus = message_bus
        self._translator = translator

    def pull(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Execute the pull use-case."""
        self._message_bus.handle(commands.PullEntities(frozenset(self._translator.to_identifiers(primary_keys))))

    def delete(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Execute the delete use-case."""
        self._message_bus.handle(commands.DeleteEntities(frozenset(self._translator.to_identifiers(primary_keys))))

    def list_idle_entities(self) -> None:
        """Execute the use-case that lists idle entities."""
        self._message_bus.handle(commands.ListIdleEntities())
