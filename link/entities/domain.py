from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import List, TypeVar, Type


@dataclass(frozen=True)
class Address:
    host: str
    database: str
    table: str


@dataclass()
class Entity:
    address: Address
    identifier: str


@dataclass()
class FlaggedEntity(Entity):
    deletion_requested: bool
    deletion_approved: bool


EntityTypeVar = TypeVar("EntityTypeVar", Entity, FlaggedEntity)


class AbstractEntityCreator(ABC):
    gateway = None

    def __init__(self) -> None:
        self.address: Address = self.gateway.address
        self.identifiers: List[str] = self.gateway.identifiers

    @property
    @abstractmethod
    def entity_cls(self) -> Type[EntityTypeVar]:
        pass

    @abstractmethod
    def _create_entities(self) -> List[EntityTypeVar]:
        pass

    def create_entities(self) -> List[EntityTypeVar]:
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
        self.deletion_approved_flags: List[bool] = self.gateway.deletion_approved_flags

    def _create_entities(self) -> List[FlaggedEntity]:
        entities = []
        for identifier, deletion_requested_flag, deletion_approved_flag in zip(
            self.identifiers, self.deletion_requested_flags, self.deletion_approved_flags
        ):
            # noinspection PyArgumentList
            entities.append(self.entity_cls(self.address, identifier, deletion_requested_flag, deletion_approved_flag))
        return entities
