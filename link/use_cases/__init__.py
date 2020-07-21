from abc import ABC, abstractmethod
from dataclasses import dataclass

from .base import UseCase
from ..entities.repository import Repository, RepositoryFactory
from ..entities.gateway import AbstractGateway
from ..entities.representation import represent


class AbstractGatewayLink(ABC):
    @property
    @abstractmethod
    def source(self) -> AbstractGateway:
        pass

    @property
    @abstractmethod
    def outbound(self) -> AbstractGateway:
        pass

    @property
    @abstractmethod
    def local(self) -> AbstractGateway:
        pass


@dataclass
class RepositoryLink:
    source: Repository
    outbound: Repository
    local: Repository


class RepositoryLinkFactory:
    repo_factory_cls = RepositoryFactory

    def __init__(self, gateway_link: AbstractGatewayLink) -> None:
        self.gateway_link = gateway_link

    def __call__(self, storage) -> RepositoryLink:
        """Creates a link."""
        kwargs = {
            kind: self.repo_factory_cls(getattr(self.gateway_link, kind), storage)()
            for kind in ("source", "outbound", "local")
        }
        return RepositoryLink(**kwargs)

    def __repr__(self) -> str:
        return represent(self, ["gateway_link"])
