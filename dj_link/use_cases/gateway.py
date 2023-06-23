"""Contains the gateway interface."""
from abc import ABC, abstractmethod

from ..entities.abstract_gateway import AbstractGateway
from ..entities.link import Link, Transfer
from ..entities.state import Update


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

    @abstractmethod
    def transfer(self, spec: Transfer) -> None:
        """Transfer an entity from one component in the link to another."""


class LinkGateway(ABC):
    """Responsible for interacting with a link's persistent data."""

    @abstractmethod
    def create_link(self) -> Link:
        """Create a link from the persistent data."""

    @abstractmethod
    def apply(self, update: Update) -> None:
        """Apply an update to the link's persistent data."""
