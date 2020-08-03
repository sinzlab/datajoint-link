from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Any

from .base import UseCase
from .pull import Pull
from ..entities.abstract_gateway import AbstractGateway
from ..entities.repository import Repository, RepositoryFactory
from ..base import Base


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
    source: Repository = None
    outbound: Repository = None
    local: Repository = None


class RepositoryLinkFactory(Base):
    repo_factory_cls = RepositoryFactory

    def __init__(self, gateway_link: AbstractGatewayLink) -> None:
        self.gateway_link = gateway_link

    def __call__(self) -> RepositoryLink:
        """Creates a link."""
        kwargs = {
            kind: self.repo_factory_cls(getattr(self.gateway_link, kind))() for kind in ("source", "outbound", "local")
        }
        return RepositoryLink(**kwargs)


def initialize(gateway_link: AbstractGatewayLink, pull_output_port: Callable[[Any], None]) -> Pull:
    """Initializes the use-cases and returns them."""
    return Pull(RepositoryLinkFactory(gateway_link), pull_output_port)
