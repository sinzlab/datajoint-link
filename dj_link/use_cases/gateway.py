"""Contains the gateway interface."""
from abc import ABC, abstractmethod

from ..entities.abstract_gateway import AbstractGateway


class GatewayLink(ABC):
    """Contains all three gateways involved in a link."""

    @property
    @abstractmethod
    def source(self) -> AbstractGateway:
        """Return the source gateway."""

    @property
    @abstractmethod
    def outbound(self) -> AbstractGateway:
        """Return the outbound gateway."""

    @property
    @abstractmethod
    def local(self) -> AbstractGateway:
        """Return the local gateway."""
