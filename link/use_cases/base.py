"""Contains the abstract base classes use-cases inherit from."""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, Generic, TypeVar

from ..base import Base

if TYPE_CHECKING:
    from . import RepositoryLinkFactory, RepositoryLink


class AbstractRequestModel(ABC):
    """ABC for request models."""


RequestModel = TypeVar("RequestModel", bound=AbstractRequestModel)


class AbstractResponseModel(ABC):
    """ABC for response models."""


class AbstractUseCase(ABC, Base, Generic[RequestModel]):
    """Specifies the interface for use-cases."""

    def __init__(
        self, repo_link_factory: RepositoryLinkFactory, output_port: Callable[[AbstractResponseModel], None]
    ) -> None:
        """Initializes the use-case."""
        self.repo_link_factory = repo_link_factory
        self.output_port = output_port

    def __call__(self, request_model: RequestModel) -> None:
        """Executes the use-case and passes the response model to the output port."""
        response_model = self.execute(self.repo_link_factory(), request_model)
        self.output_port(response_model)

    @abstractmethod
    def execute(self, repo_link: RepositoryLink, request_model: RequestModel) -> AbstractResponseModel:
        """Executes the use-case."""
