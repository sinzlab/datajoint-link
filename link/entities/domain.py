from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import List

from .address import Address


@dataclass(frozen=True)
class Entity:
    address: Address
    identifier: str


@dataclass(frozen=True)
class FlaggedEntity(Entity):
    deletion_requested: bool
    can_be_deleted: bool


class AbstractEntityCreator(ABC):
    gateway = None

    def __init__(self) -> None:
        self.address: Address = self.gateway.address
        self.identifiers: List[str] = self.gateway.identifiers

    @property
    @abstractmethod
    def entity_cls(self):
        pass

    @abstractmethod
    def _create_entities(self):
        pass

    def create_entities(self):
        return self._create_entities()

    def __repr__(self) -> str:
        return self.__class__.__qualname__ + "()"


class EntityCreator(AbstractEntityCreator):
    entity_cls = Entity

    def _create_entities(self) -> List[Entity]:
        # noinspection PyArgumentList
        return [self.entity_cls(self.address, identifier) for identifier in self.identifiers]


class FlaggedEntityCreator(AbstractEntityCreator):
    entity_cls = FlaggedEntity

    def __init__(self) -> None:
        super().__init__()
        self.deletion_requested_flags: List[bool] = self.gateway.deletion_requested_flags
        self.can_be_deleted_flags: List[bool] = self.gateway.can_be_deleted_flags

    def _create_entities(self) -> List[FlaggedEntity]:
        entities = []
        for identifier, deletion_requested_flag, can_be_deleted_flag in zip(
            self.identifiers, self.deletion_requested_flags, self.can_be_deleted_flags
        ):
            # noinspection PyArgumentList
            entities.append(self.entity_cls(self.address, identifier, deletion_requested_flag, can_be_deleted_flag))
        return entities
