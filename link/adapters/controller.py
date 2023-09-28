"""Contains code controlling the execution of use-cases."""
from __future__ import annotations

from typing import Callable, Iterable, Mapping

from link.service.services import (
    DeleteRequest,
    ListIdleEntitiesRequest,
    PullRequest,
    Request,
    Services,
)

from .custom_types import PrimaryKey
from .identification import IdentificationTranslator


class DJController:
    """Controls the execution of use-cases from DataJoint."""

    def __init__(
        self,
        handlers: Mapping[Services, Callable[[Request], None]],
        translator: IdentificationTranslator,
    ) -> None:
        """Initialize the translator."""
        self.__handlers = handlers
        self.__translator = translator

    def pull(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Execute the pull use-case."""
        self.__handlers[Services.PULL](PullRequest(frozenset(self.__translator.to_identifiers(primary_keys))))

    def delete(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Execute the delete use-case."""
        self.__handlers[Services.DELETE](DeleteRequest(frozenset(self.__translator.to_identifiers(primary_keys))))

    def list_idle_entities(self) -> None:
        """Execute the use-case that lists idle entities."""
        self.__handlers[Services.LIST_IDLE_ENTITIES](ListIdleEntitiesRequest())
