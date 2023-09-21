"""Contains code controlling the execution of use-cases."""
from __future__ import annotations

from typing import Callable, Iterable, Mapping

from dj_link.service.services import (
    DeleteRequestModel,
    ListIdleEntitiesRequestModel,
    PullRequestModel,
    RequestModel,
    UseCases,
)

from .custom_types import PrimaryKey
from .identification import IdentificationTranslator


class DJController:
    """Controls the execution of use-cases from DataJoint."""

    def __init__(
        self,
        handlers: Mapping[UseCases, Callable[[RequestModel], None]],
        translator: IdentificationTranslator,
    ) -> None:
        """Initialize the translator."""
        self.__handlers = handlers
        self.__translator = translator

    def pull(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Execute the pull use-case."""
        self.__handlers[UseCases.PULL](PullRequestModel(frozenset(self.__translator.to_identifiers(primary_keys))))

    def delete(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Execute the delete use-case."""
        self.__handlers[UseCases.DELETE](DeleteRequestModel(frozenset(self.__translator.to_identifiers(primary_keys))))

    def list_idle_entities(self) -> None:
        """Execute the use-case that lists idle entities."""
        self.__handlers[UseCases.LISTIDLEENTITIES](ListIdleEntitiesRequestModel())
