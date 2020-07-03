from abc import ABC, abstractmethod
from typing import List, TypeVar, Dict, Any


class AbstractReadOnlyGateway(ABC):
    @property
    @abstractmethod
    def identifiers(self) -> List[str]:
        pass

    @abstractmethod
    def fetch(self, identifiers: List[str]) -> Dict[str, Any]:
        pass


class AbstractGateway(AbstractReadOnlyGateway, ABC):
    @property
    @abstractmethod
    def deletion_requested_identifiers(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def deletion_approved_identifiers(self) -> List[str]:
        pass

    @abstractmethod
    def delete(self, identifiers: List[str]) -> None:
        pass

    @abstractmethod
    def insert(self, identifiers: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def start_transaction(self) -> None:
        pass

    @abstractmethod
    def commit_transaction(self) -> None:
        pass

    @abstractmethod
    def cancel_transaction(self) -> None:
        pass


class AbstractSourceGateway(AbstractReadOnlyGateway, ABC):
    pass


class AbstractOutboundGateway(AbstractGateway, ABC):
    @abstractmethod
    def approve_deletion(self, identifiers: List[str]) -> None:
        return


class AbstractLocalGateway(AbstractGateway, ABC):
    pass


GatewayTypeVar = TypeVar("GatewayTypeVar", AbstractSourceGateway, AbstractOutboundGateway, AbstractLocalGateway)
FlaggedGatewayTypeVar = TypeVar("FlaggedGatewayTypeVar", AbstractOutboundGateway, AbstractLocalGateway)
