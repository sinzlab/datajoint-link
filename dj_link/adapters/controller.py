"""Contains code controlling the execution of use-cases."""
from __future__ import annotations

from typing import Callable, Iterable, Mapping

from dj_link.entities.custom_types import Identifier
from dj_link.use_cases.use_cases import UseCases

from .custom_types import PrimaryKey
from .identification import IdentificationTranslator


class DJController:
    """Controls the execution of use-cases from DataJoint."""

    def __init__(
        self,
        handlers: Mapping[UseCases, Callable[[Iterable[Identifier]], None]],
        translator: IdentificationTranslator,
    ) -> None:
        """Initialize the translator."""
        self.__handlers = handlers
        self.__translator = translator

    def pull(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Execute the pull use-case."""
        self.__handlers[UseCases.PULL](self.__translator.to_identifiers(primary_keys))

    def delete(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Execute the delete use-case."""
        self.__handlers[UseCases.DELETE](self.__translator.to_identifiers(primary_keys))
