from __future__ import annotations
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Dict, Iterator

from ..base import Base

if TYPE_CHECKING:
    from .abstract_gateway import AbstractEntityGateway
    from .repository import Entity


class Contents(MutableMapping, Base):
    def __init__(self, entities: Dict[str, Entity], gateway: AbstractEntityGateway) -> None:
        self.entities = entities
        self.gateway = gateway

    def __getitem__(self, identifier: str) -> Entity:
        """Fetches an entity."""
        entity = self.entities[identifier]
        entity.data = self.gateway.fetch(identifier)
        return entity

    def __setitem__(self, identifier: str, entity: Entity) -> None:
        """Inserts an entity."""
        self.gateway.insert(entity.data)
        self.entities[identifier] = entity

    def __delitem__(self, identifier: str) -> None:
        """Deletes an entity."""
        self.gateway.delete(identifier)
        del self.entities[identifier]

    def __iter__(self) -> Iterator[str]:
        """Iterates over entity identifiers."""
        return iter(self.entities)

    def __len__(self) -> int:
        """Returns the number of entities in the repository."""
        return len(self.entities)
