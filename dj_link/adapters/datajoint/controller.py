"""Contains code controlling the execution of use-cases."""
from __future__ import annotations

from typing import Callable, Iterable, Mapping

from dj_link.custom_types import PrimaryKey
from dj_link.entities.custom_types import Identifier
from dj_link.use_cases.use_cases import UseCases

from ...base import Base
from ...use_cases import RequestModelClasses
from ...use_cases.base import AbstractUseCase
from .gateway import DataJointGatewayLink
from .identification import IdentificationTranslator


class Controller(Base):
    """Controls the execution of use-cases at the user's request."""

    def __init__(
        self,
        use_cases: Mapping[str, AbstractUseCase],
        request_model_classes: RequestModelClasses,
        gateway_link: DataJointGatewayLink,
    ) -> None:
        """Initialize the controller."""
        self.use_cases = use_cases
        self.request_model_classes = request_model_classes
        self.gateway_link = gateway_link

    def pull(self, restriction) -> None:
        """Pull the requested entities from the source table into the local table."""
        identifiers = self.gateway_link.source.get_identifiers_in_restriction(restriction)
        # noinspection PyArgumentList
        self.use_cases["pull"](self.request_model_classes["pull"](identifiers))

    def delete(self, restriction) -> None:
        """Delete the requested entities from the local table."""
        identifiers = self.gateway_link.local.get_identifiers_in_restriction(restriction)
        # noinspection PyArgumentList
        self.use_cases["delete"](self.request_model_classes["delete"](identifiers))

    def refresh(self) -> None:
        """Refresh the repositories."""
        self.use_cases["refresh"](self.request_model_classes["refresh"]())


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
