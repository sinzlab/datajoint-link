from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Any, Dict, Type

from .base import AbstractUseCase
from .pull import PullRequestModel, PullResponseModel, PullUseCase
from .delete import DeleteRequestModel, DeleteResponseModel, DeleteUseCase
from .refresh import RefreshRequestModel, RefreshResponseModel, RefreshUseCase
from ..entities.abstract_gateway import AbstractGateway
from ..entities.repository import Repository, RepositoryFactory
from ..base import Base


REQUEST_MODELS = dict(pull=PullRequestModel, delete=DeleteRequestModel, refresh=RefreshRequestModel)
RESPONSE_MODELS = dict(pull=PullResponseModel, delete=DeleteResponseModel, refresh=RefreshResponseModel)
USE_CASES: Dict[str, Type[AbstractUseCase]] = dict(pull=PullUseCase, delete=DeleteUseCase, refresh=RefreshUseCase)


class AbstractGatewayLink(ABC):
    @property
    @abstractmethod
    def source(self) -> AbstractGateway:
        """Returns the source gateway."""

    @property
    @abstractmethod
    def outbound(self) -> AbstractGateway:
        """Returns the outbound gateway."""

    @property
    @abstractmethod
    def local(self) -> AbstractGateway:
        """Returns the local gateway."""


@dataclass
class RepositoryLink:
    source: Repository
    outbound: Repository
    local: Repository


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
    gateway_link: AbstractGatewayLink, output_ports: Dict[str, Callable[[Any], None]]
) -> Dict[str, AbstractUseCase]:
    """Initializes the use-cases and returns them."""
    factory = RepositoryLinkFactory(gateway_link)
    return {n: uc(factory, output_ports[n]) for n, uc in USE_CASES.items()}
