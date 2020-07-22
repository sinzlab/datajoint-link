from __future__ import annotations
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Dict, Iterator

from .representation import represent

if TYPE_CHECKING:
    from .gateway import AbstractGateway
    from .repository import Entity


class Contents(MutableMapping):
    def __init__(self, entities: Dict[str, Entity], gateway: AbstractGateway, storage: Dict) -> None:
        self.entities = entities
        self.gateway = gateway
        self.storage = storage

    def __getitem__(self, identifier: str) -> Entity:
        """Fetches an entity."""
        data = self.gateway.fetch(identifier)
        self.storage[identifier] = data
        return self.entities[identifier]

    def __setitem__(self, identifier: str, entity: Entity) -> None:
        """Inserts an entity."""
        data = self.storage[identifier]
        self.gateway.insert(data)
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

    def __repr__(self) -> str:
        return represent(self, ["entities", "gateway", "storage"])
