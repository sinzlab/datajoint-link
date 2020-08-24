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
        pass

    @property
    @abstractmethod
    def outbound(self) -> AbstractTableFacade:
        pass

    @property
    @abstractmethod
    def local(self) -> AbstractTableFacade:
        pass


class DataJointGatewayLink(AbstractGatewayLink, Base):
    source = outbound = local = None

    def __init__(self, source: DataJointGateway, outbound: DataJointGateway, local: DataJointGateway):
        self.source = source
        self.outbound = outbound
        self.local = local


def initialize(table_facade_link: AbstractTableFacadeLink) -> DataJointGatewayLink:
    gateways = {}
    for kind in ("source", "outbound", "local"):
        table_facade = getattr(table_facade_link, kind)
        gateways[kind] = DataJointGateway(table_facade, IdentificationTranslator(table_facade))
    return DataJointGatewayLink(**gateways)
