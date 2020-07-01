from abc import ABC, abstractmethod
from typing import List, TypeVar


class AbstractGateway(ABC):
    @property
    @abstractmethod
    def identifiers(self) -> List[str]:
        pass

    @abstractmethod
    def fetch(self, identifiers: List[str]) -> None:
        pass

    @abstractmethod
    def delete(self, identifiers: List[str]) -> None:
        pass

    @abstractmethod
    def insert(self, identifiers: List[str]) -> None:
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


class AbstractFlaggedGateway(AbstractGateway, ABC):
    @property
    @abstractmethod
    def deletion_requested_flags(self) -> List[bool]:
        pass

    @property
    @abstractmethod
    def deletion_approved_flags(self) -> List[bool]:
        pass


class AbstractSourceGateway(AbstractGateway, ABC):
    pass


class AbstractOutboundGateway(AbstractFlaggedGateway, ABC):
    @property
    def approve_deletion(self):
        return


class AbstractLocalGateway(AbstractFlaggedGateway, ABC):
    pass


GatewayTypeVar = TypeVar("GatewayTypeVar", AbstractSourceGateway, AbstractOutboundGateway, AbstractLocalGateway)
FlaggedGatewayTypeVar = TypeVar("FlaggedGatewayTypeVar", AbstractOutboundGateway, AbstractLocalGateway)
