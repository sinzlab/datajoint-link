from __future__ import annotations
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, TypeVar, Type

if TYPE_CHECKING:
    from ..adapters.gateway import GatewayTypeVar, AbstractSourceGateway, FlaggedGatewayTypeVar


@dataclass
class Entity:
    identifier: str


@dataclass
class ManagedEntity(Entity):
    deletion_requested: bool


@dataclass
class SourceEntity(Entity):
    pass


@dataclass
class OutboundEntity(ManagedEntity):
    deletion_approved: bool


@dataclass
class LocalEntity(ManagedEntity):
    pass


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
        for identifier in self.gateway.identifiers:
            # noinspection PyArgumentList
            entities.append(
                self.entity_cls(
                    identifier,
                    True if identifier in self.gateway.deletion_requested_identifiers else False,
                    True if identifier in self.gateway.deletion_approved_identifiers else False,
                )
            )
        return entities


EntityCreatorTypeVar = TypeVar("EntityCreatorTypeVar", EntityCreator, FlaggedEntityCreator)
