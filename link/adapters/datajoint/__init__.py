from abc import ABC, abstractmethod

from .gateway import DataJointGateway
from .proxy import AbstractTableProxy
from .identification import IdentificationTranslator
from ...entities.representation import represent
from ...use_cases import AbstractGatewayLink


class AbstractTableProxyLink(ABC):
    @property
    @abstractmethod
    def source(self) -> AbstractTableProxy:
        pass

    @property
    @abstractmethod
    def outbound(self) -> AbstractTableProxy:
        pass

    @property
    @abstractmethod
    def local(self) -> AbstractTableProxy:
        pass


class DataJointGatewayLink(AbstractGatewayLink):
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

    def __repr__(self) -> str:
        return represent(self, ["source_gateway", "outbound_gateway", "local_gateway"])


def initialize(table_proxy_link: AbstractTableProxyLink) -> DataJointGatewayLink:
    gateways = dict()
    for kind in ("source", "outbound", "local"):
        table_proxy = getattr(table_proxy_link, kind)
        gateways[kind + "_gateway"] = DataJointGateway(table_proxy, IdentificationTranslator(table_proxy))
    return DataJointGatewayLink(**gateways)
