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
    def __init__(
        self, source_gateway: DataJointGateway, outbound_gateway: DataJointGateway, local_gateway: DataJointGateway
    ):
        self.source_gateway = source_gateway
        self.outbound_gateway = outbound_gateway
        self.local_gateway = local_gateway

    @property
    def source(self) -> DataJointGateway:
        return self.source_gateway

    @property
    def outbound(self) -> DataJointGateway:
        return self.outbound_gateway

    @property
    def local(self) -> DataJointGateway:
        return self.local_gateway


def initialize(table_facade_link: AbstractTableFacadeLink) -> DataJointGatewayLink:
    gateways = dict()
    for kind in ("source", "outbound", "local"):
        table_facade = getattr(table_facade_link, kind)
        gateways[kind + "_gateway"] = DataJointGateway(table_facade, IdentificationTranslator(table_facade))
    return DataJointGatewayLink(**gateways)
