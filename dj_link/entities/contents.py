"""Contains code pertaining to the fetching, inserting and deleting of entities into/from repositories."""
from __future__ import annotations

from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Iterator

from ..base import Base
from .abstract_gateway import AbstractGateway
from .custom_types import Identifier

if TYPE_CHECKING:
    from .repository import EntityFactory, TransferEntity


class Contents(MutableMapping, Base):
    """Handles the fetching, inserting and deleting of entities into/from repositories."""

    def __init__(self, gateway: AbstractGateway, entity_factory: EntityFactory) -> None:
        """Initialize the contents."""
        self.gateway = gateway
        self.entity_factory = entity_factory

    def __getitem__(self, identifier: Identifier) -> TransferEntity:
        """Fetch an entity."""
        entity = self.entity_factory(identifier)
        return entity.create_transfer_entity(self.gateway.fetch(identifier))

    def __setitem__(self, identifier: Identifier, transfer_entity: TransferEntity) -> None:
        """Insert an entity."""
        self.gateway.insert(transfer_entity.data)

    def __delitem__(self, identifier: Identifier) -> None:
        """Delete an entity."""
        self.gateway.delete(identifier)

    def __iter__(self) -> Iterator[Identifier]:
        """Iterate over entity identifiers."""
        return iter(self.gateway)

    def __len__(self) -> int:
        """Return the number of entities in the repository."""
        return len(self.gateway)
