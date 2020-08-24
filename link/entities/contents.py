from __future__ import annotations
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Dict, Iterator

from ..base import Base
from .abstract_gateway import AbstractGateway

if TYPE_CHECKING:
    from .repository import Entity, TransferEntity


class Contents(MutableMapping, Base):
    def __init__(self, entities: Dict[str, Entity], gateway: AbstractGateway) -> None:
        self.entities = entities
        self.gateway = gateway

    def __getitem__(self, identifier: str) -> TransferEntity:
        """Fetches an entity."""
        entity = self.entities[identifier]
        return entity.create_transfer_entity(self.gateway.fetch(identifier))

    def __setitem__(self, identifier: str, transfer_entity: TransferEntity) -> None:
        """Inserts an entity."""
        self.gateway.insert(transfer_entity.data)
        self.entities[identifier] = transfer_entity.create_entity()

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
