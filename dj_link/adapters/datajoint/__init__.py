from abc import ABC, abstractmethod

from .gateway import DataJointGateway
from .abstract_facade import AbstractTableFacade
from .identification import IdentificationTranslator
from ...base import Base
from ...use_cases import AbstractGatewayLink


class AbstractTableFacadeLink(ABC):
    @property
    @abstractmethod
    def source(self) -> AbstractTableFacade:
        """Returns the table facade corresponding to the source table."""

    @property
    @abstractmethod
    def outbound(self) -> AbstractTableFacade:
        """Returns the table facade corresponding to the outbound table."""

    @property
    @abstractmethod
    def local(self) -> AbstractTableFacade:
        """Returns the table facade corresponding to the local table."""


class DataJointGatewayLink(AbstractGatewayLink, Base):
    def __init__(self, source: DataJointGateway, outbound: DataJointGateway, local: DataJointGateway):
        self._source = source
        self._outbound = outbound
        self._local = local

    @property
    def source(self) -> DataJointGateway:
        return self._source

    @property
    def outbound(self) -> DataJointGateway:
        return self._outbound

    @property
    def local(self) -> DataJointGateway:
        return self._local


def initialize(table_facade_link: AbstractTableFacadeLink) -> DataJointGatewayLink:
    gateways = {}
    for kind in ("source", "outbound", "local"):
        table_facade = getattr(table_facade_link, kind)
        gateways[kind] = DataJointGateway(table_facade, IdentificationTranslator(table_facade))
    return DataJointGatewayLink(**gateways)
