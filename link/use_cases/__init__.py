from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Callable, Any

from .base import UseCase
from .pull import Pull
from ..entities.repository import Repository, RepositoryFactory
from ..entities.abstract_gateway import AbstractGateway
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

    def __call__(self) -> RepositoryLink:
        """Creates a link."""
        kwargs = {
            kind: self.repo_factory_cls(getattr(self.gateway_link, kind))() for kind in ("source", "outbound", "local")
        }
        return RepositoryLink(**kwargs)

    def __repr__(self) -> str:
        return represent(self, ["gateway_link"])


def initialize(gateway_link: AbstractGatewayLink, output_ports: Dict[str, Callable[[Any], None]]) -> Dict[str, UseCase]:
    """Initializes the use-cases and returns them."""
    return dict(pull=Pull(RepositoryLinkFactory(gateway_link), output_ports["pull"]))
