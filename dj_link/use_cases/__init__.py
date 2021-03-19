"""Contains code Initializing the use-cases."""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, Type, TypedDict

from ..base import Base
from ..entities.abstract_gateway import AbstractGateway
from ..entities.repository import Repository, RepositoryFactory
from .base import AbstractUseCase
from .delete import DeleteRequestModel, DeleteResponseModel, DeleteUseCase
from .pull import PullRequestModel, PullResponseModel, PullUseCase
from .refresh import RefreshRequestModel, RefreshResponseModel, RefreshUseCase


class RequestModelClasses(TypedDict):
    """Maps use-case names to the corresponding request model class for that use-case."""

    pull: Type[PullRequestModel]
    delete: Type[DeleteRequestModel]
    refresh: Type[RefreshRequestModel]


REQUEST_MODELS = RequestModelClasses(pull=PullRequestModel, delete=DeleteRequestModel, refresh=RefreshRequestModel)
RESPONSE_MODELS = dict(pull=PullResponseModel, delete=DeleteResponseModel, refresh=RefreshResponseModel)
USE_CASES: Dict[str, Type[AbstractUseCase]] = dict(pull=PullUseCase, delete=DeleteUseCase, refresh=RefreshUseCase)


class AbstractGatewayLink(ABC):
    """Contains all three gateways involved in a link."""

    @property
    @abstractmethod
    def source(self) -> AbstractGateway:
        """Return the source gateway."""

    @property
    @abstractmethod
    def outbound(self) -> AbstractGateway:
        """Return the outbound gateway."""

    @property
    @abstractmethod
    def local(self) -> AbstractGateway:
        """Return the local gateway."""


@dataclass
class RepositoryLink:
    """Contains all three repositories involved in a link."""

    source: Repository
    outbound: Repository
    local: Repository


class RepositoryLinkFactory(Base):  # pylint: disable=too-few-public-methods
    """Factory that produces repository links."""

    repo_factory_cls = RepositoryFactory

    def __init__(self, gateway_link: AbstractGatewayLink) -> None:
        """Initialize the repository link factory."""
        self.gateway_link = gateway_link

    def __call__(self) -> RepositoryLink:
        """Create a link."""
        kwargs = {
            kind: self.repo_factory_cls(getattr(self.gateway_link, kind))() for kind in ("source", "outbound", "local")
        }
        return RepositoryLink(**kwargs)


def initialize_use_cases(
    gateway_link: AbstractGatewayLink, output_ports: Dict[str, Callable[[Any], None]]
) -> Dict[str, AbstractUseCase]:
    """Initialize the use-cases and returns them."""
    factory = RepositoryLinkFactory(gateway_link)
    return {n: uc(factory, output_ports[n]) for n, uc in USE_CASES.items()}
