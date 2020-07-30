from abc import ABC, abstractmethod
from typing import Any, Optional

from ..entities.abstract_gateway import AbstractEntityGateway as AbstractEntityGateway


class AbstractEntityDTO(ABC):
    """Defines the interface of the data transfer object that is passed between the gateway and the use-cases."""

    @property
    @abstractmethod
    def identifier_data(self) -> Any:
        """Contains all the data used to compute the unique identifier of the entity."""

    @property
    @abstractmethod
    def non_identifier_data(self) -> Optional[Any]:
        """Contains all the data not used to compute the unique identifier of the entity."""


class AbstractUseCaseGateway(AbstractEntityGateway, ABC):
    """Defines the gateway interface as expected by the use-cases."""

    @abstractmethod
    def fetch(self, identifier: str) -> AbstractEntityDTO:
        """Fetches an entity."""

    @abstractmethod
    def insert(self, entity_dto: AbstractEntityDTO) -> None:
        """Inserts an entity."""
