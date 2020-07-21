"""Contains the abstract base classes use-cases inherit from."""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, Any

from ..entities.representation import represent

if TYPE_CHECKING:
    from . import RepositoryLinkFactory


class UseCase(ABC):
    """Specifies the interface for use-cases."""

    def __init__(self, repo_link_factory: RepositoryLinkFactory, output_port: Callable[[Any], None]) -> None:
        """Initializes the use-case."""
        self.repo_link_factory = repo_link_factory
        self.output_port = output_port

    def __call__(self, *args, **kwargs) -> None:
        """Executes the use-case and passes its output to the output port."""
        output = self.execute(self.repo_link_factory(dict()), *args, **kwargs)
        self.output_port(output)

    def __repr__(self) -> str:
        return represent(self, ["repo_link_factory", "output_port"])

    @abstractmethod
    def execute(self, *args, **kwargs) -> Any:
        """Executes the use-case."""
