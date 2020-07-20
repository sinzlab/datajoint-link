from collections.abc import MutableMapping
from typing import Dict, Iterator, Optional
from dataclasses import dataclass, field

from .gateway import AbstractGateway
from .representation import _represent


@dataclass
class Entity:
    identifier: str
    flags: Optional[Dict[str, bool]] = field(default_factory=dict)


class Repository(MutableMapping):
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
        self.gateway.insert(identifier, data)
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
        return _represent(self, ["entities", "gateway", "storage"])
