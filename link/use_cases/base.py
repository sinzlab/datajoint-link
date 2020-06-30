"""Contains the abstract base classes use-cases inherit from."""
from abc import ABC, abstractmethod
from typing import Callable, Any


class UseCase(ABC):
    """Specifies the interface for use-cases."""

    def __init__(self, output_port: Callable[[Any], None]) -> None:
        """Initializes the use-case."""
        self.output_port = output_port

    def __call__(self, *args, **kwargs) -> None:
        """Executes the use-case and passes its output to the output port."""
        output = self.execute(*args, **kwargs)
        self.output_port(output)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({self.output_port})"

    @abstractmethod
    def execute(self, *args, **kwargs):
        """Executes the use-case."""
