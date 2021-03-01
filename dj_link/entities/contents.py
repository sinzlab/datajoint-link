"""Contains code pertaining to the fetching, inserting and deleting of entities into/from repositories."""
from __future__ import annotations

from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Dict, Iterator

from ..base import Base
from .abstract_gateway import AbstractGateway

if TYPE_CHECKING:
    from .repository import Entity, TransferEntity


class Contents(MutableMapping, Base):
    """Handles the fetching, inserting and deleting of entities into/from repositories."""

    def __init__(self, entities: Dict[str, Entity], gateway: AbstractGateway) -> None:
        """Initialize the contents."""
        self.entities = entities
        self.gateway = gateway

    def __getitem__(self, identifier: str) -> TransferEntity:
        """Fetch an entity."""
        entity = self.entities[identifier]
        return entity.create_transfer_entity(self.gateway.fetch(identifier))

    def __setitem__(self, identifier: str, transfer_entity: TransferEntity) -> None:
        """Insert an entity."""
        self.gateway.insert(transfer_entity.data)
        self.entities[identifier] = transfer_entity.create_entity()

    def __delitem__(self, identifier: str) -> None:
        """Delete an entity."""
        self.gateway.delete(identifier)
        del self.entities[identifier]

    def __iter__(self) -> Iterator[str]:
        """Iterate over entity identifiers."""
        return iter(self.entities)

    def __len__(self) -> int:
        """Return the number of entities in the repository."""
        return len(self.entities)
