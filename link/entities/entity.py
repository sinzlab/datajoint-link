from __future__ import annotations
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, TypeVar, Type

if TYPE_CHECKING:
    from ..adapters.gateway import GatewayTypeVar, AbstractSourceGateway, FlaggedGatewayTypeVar


@dataclass()
class Entity:
    identifier: str


@dataclass()
class FlaggedEntity(Entity):
    deletion_requested: bool
    deletion_approved: bool


EntityTypeVar = TypeVar("EntityTypeVar", Entity, FlaggedEntity)


class AbstractEntityCreator(ABC):
    def __init__(self, gateway: GatewayTypeVar) -> None:
        self.gateway = gateway

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
    gateway: AbstractSourceGateway

    def _create_entities(self) -> List[Entity]:
        # noinspection PyArgumentList
        return [self.entity_cls(identifier) for identifier in self.gateway.identifiers]


class FlaggedEntityCreator(AbstractEntityCreator):
    entity_cls = FlaggedEntity
    gateway: FlaggedGatewayTypeVar

    def _create_entities(self) -> List[FlaggedEntity]:
        entities = []
        for identifier, deletion_requested_flag, deletion_approved_flag in zip(
            self.gateway.identifiers, self.gateway.deletion_requested_flags, self.gateway.deletion_approved_flags
        ):
            # noinspection PyArgumentList
            entities.append(self.entity_cls(identifier, deletion_requested_flag, deletion_approved_flag))
        return entities


EntityCreatorTypeVar = TypeVar("EntityCreatorTypeVar", EntityCreator, FlaggedEntityCreator)
