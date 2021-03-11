"""Contains code initializing the adapters."""
from abc import ABC, abstractmethod

from ...base import Base
from ...use_cases import AbstractGatewayLink
from .abstract_facade import AbstractTableFacade
from .gateway import DataJointGateway
from .identification import IdentificationTranslator


class AbstractTableFacadeLink(ABC):
    """Contains the three DataJoint table facades corresponding to the three table types."""

    @property
    @abstractmethod
    def source(self) -> AbstractTableFacade:
        """Return the table facade corresponding to the source table."""

    @property
    @abstractmethod
    def outbound(self) -> AbstractTableFacade:
        """Return the table facade corresponding to the outbound table."""

    @property
    @abstractmethod
    def local(self) -> AbstractTableFacade:
        """Return the table facade corresponding to the local table."""


class DataJointGatewayLink(AbstractGatewayLink, Base):
    """Contains the three DataJoint gateways corresponding to the three table types."""

    def __init__(self, source: DataJointGateway, outbound: DataJointGateway, local: DataJointGateway):
        """Initialize the DataJoint gateway link."""
        self._source = source
        self._outbound = outbound
        self._local = local

    @property
    def source(self) -> DataJointGateway:
        """Return the source gateway."""
        return self._source

    @property
    def outbound(self) -> DataJointGateway:
        """Return the outbound gateway."""
        return self._outbound

    @property
    def local(self) -> DataJointGateway:
        """Return the local gateway."""
        return self._local


def initialize(table_facade_link: AbstractTableFacadeLink) -> DataJointGatewayLink:
    """Initialize the adapters."""
    gateways = {}
    for kind in ("source", "outbound", "local"):
        table_facade = getattr(table_facade_link, kind)
        gateways[kind] = DataJointGateway(table_facade, IdentificationTranslator())
    return DataJointGatewayLink(**gateways)
