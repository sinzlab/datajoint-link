"""Contains the abstract base classes use-cases inherit from."""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, Any

from ..base import Base

if TYPE_CHECKING:
    from . import RepositoryLinkFactory, RepositoryLink


class UseCase(ABC, Base):
    """Specifies the interface for use-cases."""

    def __init__(self, repo_link_factory: RepositoryLinkFactory, output_port: Callable[[Any], None]) -> None:
        """Initializes the use-case."""
        self.repo_link_factory = repo_link_factory
        self.output_port = output_port

    def __call__(self, *args, **kwargs) -> None:
        """Executes the use-case and passes its output to the output port."""
        output = self.execute(self.repo_link_factory(), *args, **kwargs)
        self.output_port(output)

    @abstractmethod
    def execute(self, repo_link: RepositoryLink, *args, **kwargs) -> Any:
        """Executes the use-case."""
