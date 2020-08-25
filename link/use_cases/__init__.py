from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Any, Dict

from .base import UseCase
from .pull import PullResponseModel, PullUseCase
from .delete import DeleteResponseModel, DeleteUseCase
from .refresh import RefreshResponseModel, RefreshUseCase
from ..entities.abstract_gateway import AbstractGateway
from ..entities.repository import Repository, RepositoryFactory
from ..base import Base


RESPONSE_MODELS = dict(pull=PullResponseModel, delete=DeleteResponseModel, refresh=RefreshResponseModel)
USE_CASES = dict(pull=PullUseCase, delete=DeleteUseCase, refresh=RefreshUseCase)


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


def initialize_use_cases(
    gateway_link: AbstractGatewayLink, output_ports: Dict[str, Callable[[Any], None]],
) -> Dict[str, UseCase]:
    """Initializes the use-cases and returns them."""
    factory = RepositoryLinkFactory(gateway_link)
    return {n: uc(factory, output_ports[n]) for n, uc in USE_CASES.items()}
