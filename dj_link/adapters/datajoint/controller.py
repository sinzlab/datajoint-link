"""Contains code controlling the execution of use-cases."""
from typing import Mapping

from ...base import Base
from ...use_cases import RequestModelClasses
from ...use_cases.base import AbstractUseCase
from .gateway import DataJointGateway


class Controller(Base):
    """Controls the execution of use-cases at the user's request."""

    def __init__(
        self,
        use_cases: Mapping[str, AbstractUseCase],
        request_model_classes: RequestModelClasses,
        gateways: Mapping[str, DataJointGateway],
    ) -> None:
        """Initialize the controller."""
        self.use_cases = use_cases
        self.request_model_classes = request_model_classes
        self.gateways = gateways

    def pull(self, restriction) -> None:
        """Pull the requested entities from the source table into the local table."""
        identifiers = self.gateways["source"].get_identifiers_in_restriction(restriction)
        # noinspection PyArgumentList
        self.use_cases["pull"](self.request_model_classes["pull"](identifiers))

    def delete(self, restriction) -> None:
        """Delete the requested entities from the local table."""
        identifiers = self.gateways["local"].get_identifiers_in_restriction(restriction)
        # noinspection PyArgumentList
        self.use_cases["delete"](self.request_model_classes["delete"](identifiers))

    def refresh(self) -> None:
        """Refresh the repositories."""
        self.use_cases["refresh"](self.request_model_classes["refresh"]())
