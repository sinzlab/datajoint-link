"""Contains the abstract base classes use-cases inherit from."""
from abc import ABC, abstractmethod
from typing import Callable, Any


class InitializationUseCase(ABC):
    """ABC for use-cases that deal with initialization."""

    def __init__(self, output_port: Callable[[Any], None]) -> None:
        """Initializes the use-case."""
        self.output_port = output_port

    def __call__(self, *args, **kwargs) -> None:
        """Executes the use-case and passes its output to the output port."""
        output = self.execute(*args, **kwargs)
        self.output_port(output)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({repr(self.output_port)})"

    @abstractmethod
    def execute(self, *args, **kwargs):
        """Executes the use-case."""


class UseCase(InitializationUseCase):
    """ABC for all use-cases invoked by the end user."""

    initialize_local: InitializationUseCase = None
    initialize_source: InitializationUseCase = None

    @property
    @abstractmethod
    def requires_local(self) -> bool:
        """Whether the execution of the use-case requires a connection to the local side."""

    @property
    @abstractmethod
    def requires_source(self) -> bool:
        """Whether the execution of the use-case requires a connection to the source side."""

    def __call__(self, *args, **kwargs) -> None:
        """Initializes the local and/or source side(s) if required and then executes the use-case."""
        if self.requires_local:
            self.initialize_local()
        if self.requires_source:
            self.initialize_source()
        super().__call__(*args, **kwargs)
